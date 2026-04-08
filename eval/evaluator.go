package eval

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hinskii/promclick/clickhouse"
	"github.com/hinskii/promclick/config"
	"github.com/hinskii/promclick/fingerprint"
	"github.com/hinskii/promclick/translator"
	"github.com/hinskii/promclick/types"
)

// DataFetcher is the interface for fetching series data from ClickHouse.
type DataFetcher interface {
	FetchSeriesData(ctx context.Context, sql string, params *clickhouse.QueryParams) (map[string]*clickhouse.SeriesData, error)
}

// Evaluator performs Go-side evaluation of PromQL functions.
type Evaluator struct {
	cfg     *config.Config
	client  *clickhouse.Client // legacy HTTP client
	fetcher DataFetcher        // interface (HTTP or Native TCP)
}

// New creates an Evaluator without a client (for sql-only mode).
func New(cfg *config.Config) *Evaluator {
	return &Evaluator{cfg: cfg}
}

// NewWithClient creates an Evaluator with the HTTP ClickHouse client.
func NewWithClient(cfg *config.Config, client *clickhouse.Client) *Evaluator {
	return &Evaluator{cfg: cfg, client: client, fetcher: client}
}

// NewWithFetcher creates an Evaluator with a custom DataFetcher (e.g. native TCP pool).
func NewWithFetcher(cfg *config.Config, fetcher DataFetcher) *Evaluator {
	return &Evaluator{cfg: cfg, fetcher: fetcher}
}

// EvalPlan fetches data from ClickHouse and evaluates the plan.
// Handles binary expressions by recursively evaluating LHS and RHS.
func (ev *Evaluator) EvalPlan(
	ctx context.Context,
	plan *translator.SQLPlan,
	start, end time.Time,
	step time.Duration,
) (*types.QueryResult, error) {

	if plan.IsScalar {
		return &types.QueryResult{
			Type:   "vector",
			Vector: types.Vector{{F: plan.ScalarVal, T: start.UnixMilli()}},
		}, nil
	}

	// time() — returns eval timestamp as float seconds per step
	if plan.ExprType == "time" {
		if step <= 0 {
			return &types.QueryResult{
				Type:   "vector",
				Vector: types.Vector{{F: float64(end.Unix()), T: end.UnixMilli()}},
			}, nil
		}
		steps := generateSteps(start, end, step)
		var series types.Series
		for _, t := range steps {
			series.Samples = append(series.Samples, types.Sample{Timestamp: t, Value: float64(t) / 1000.0})
		}
		return &types.QueryResult{Type: "matrix", Matrix: types.Matrix{series}}, nil
	}

	// Binary: evaluate LHS and RHS separately, then combine
	if plan.ExprType == "binary" {
		return ev.evalBinaryPlan(ctx, plan, start, end, step)
	}

	// Select tier (MV or raw)
	queryRange := end.Sub(start)
	_, tier := ev.cfg.SelectTier(step, queryRange)
	useMV := tier != nil && isMVCompatible(plan, tier)

	var sql string
	var params *clickhouse.QueryParams
	if useMV {
		sql, params = plan.RenderMV(ev.cfg, tier)
	} else {
		sql, params = plan.Render()
	}
	if ev.cfg.Output.ShowSQL {
		fmt.Fprintf(os.Stderr, "-- SQL:\n%s\n\n", sql)
	}

	seriesMap, err := ev.fetcher.FetchSeriesData(ctx, sql, params)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}

	if ev.cfg.Prometheus.MaxSeries > 0 && len(seriesMap) > ev.cfg.Prometheus.MaxSeries {
		return nil, &TooManySeriesError{Count: len(seriesMap), Max: ev.cfg.Prometheus.MaxSeries}
	}

	stalenessMs := int64(ev.cfg.Prometheus.StalenessSeconds) * 1000
	if useMV {
		// MV data: widen staleness to tier resolution
		stalenessMs = tier.Resolution.Milliseconds() * 2
	}
	steps := generateSteps(start, end, step)

	// For MV: data is pre-aggregated, use plain InstantValue
	evalPlan := plan
	if useMV {
		mvPlan := *plan
		mvPlan.FuncName = "" // skip *_over_time computation — value is ready
		mvPlan.RangeMs = 0
		evalPlan = &mvPlan
	}

	if len(steps) == 1 {
		vec := ev.evalInstantSD(seriesMap, evalPlan, steps[0], stalenessMs)
		return &types.QueryResult{Type: "vector", Vector: vec}, nil
	}

	matrix := ev.buildMatrixSD(seriesMap, evalPlan, steps, stalenessMs)
	return &types.QueryResult{Type: "matrix", Matrix: matrix}, nil
}

// evalBinaryPlan evaluates a binary expression by fetching LHS and RHS separately.
func (ev *Evaluator) evalBinaryPlan(
	ctx context.Context,
	plan *translator.SQLPlan,
	start, end time.Time,
	step time.Duration,
) (*types.QueryResult, error) {
	// Handle scalar RHS (e.g. expr > 0, exp(rate/1000))
	if plan.IsScalarRHS {
		lhsRes, err := ev.EvalPlan(ctx, plan.LHS, start, end, step)
		if err != nil {
			return nil, fmt.Errorf("binary lhs: %w", err)
		}
		result := applyScalarBinary(lhsRes, plan.ScalarRHS, plan.BinaryOp, plan.ReturnBool, false)
		if len(plan.MathChain) > 0 {
			applyMathChainResult(result, plan.MathChain)
		}
		return result, nil
	}
	// Handle scalar LHS (e.g. 0 < expr)
	if plan.IsScalarLHS {
		rhsRes, err := ev.EvalPlan(ctx, plan.RHS, start, end, step)
		if err != nil {
			return nil, fmt.Errorf("binary rhs: %w", err)
		}
		result := applyScalarBinary(rhsRes, plan.ScalarLHS, plan.BinaryOp, plan.ReturnBool, true)
		if len(plan.MathChain) > 0 {
			applyMathChainResult(result, plan.MathChain)
		}
		return result, nil
	}

	type res struct {
		result *types.QueryResult
		err    error
	}
	lhsCh := make(chan res, 1)
	rhsCh := make(chan res, 1)

	go func() {
		r, err := ev.EvalPlan(ctx, plan.LHS, start, end, step)
		lhsCh <- res{r, err}
	}()
	go func() {
		r, err := ev.EvalPlan(ctx, plan.RHS, start, end, step)
		rhsCh <- res{r, err}
	}()

	var lhsRes, rhsRes res
	var gotLHS, gotRHS bool
	for i := 0; i < 2; i++ {
		select {
		case r := <-lhsCh:
			lhsRes = r
			gotLHS = true
		case r := <-rhsCh:
			rhsRes = r
			gotRHS = true
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if !gotLHS || !gotRHS {
		return nil, ctx.Err()
	}
	if lhsRes.err != nil {
		return nil, fmt.Errorf("binary lhs: %w", lhsRes.err)
	}
	if rhsRes.err != nil {
		return nil, fmt.Errorf("binary rhs: %w", rhsRes.err)
	}

	// If either side is scalar_func, treat as scalar binary
	if isScalarResult(rhsRes.result) {
		scalarVal := extractScalarValue(rhsRes.result)
		return applyScalarBinary(lhsRes.result, scalarVal, plan.BinaryOp, plan.ReturnBool, false), nil
	}
	if isScalarResult(lhsRes.result) {
		scalarVal := extractScalarValue(lhsRes.result)
		return applyScalarBinary(rhsRes.result, scalarVal, plan.BinaryOp, plan.ReturnBool, true), nil
	}

	vm := VectorMatching{Card: "one-to-one"}
	if plan.VectorMatching != nil {
		vm = VectorMatching{
			Card:           plan.VectorMatching.Card,
			MatchingLabels: plan.VectorMatching.MatchingLabels,
			On:             plan.VectorMatching.On,
			Include:        plan.VectorMatching.Include,
		}
	}

	steps := generateSteps(start, end, step)

	// Collect vectors per step from both sides
	lhsVecByStep := vecsByStep(lhsRes.result, steps)
	rhsVecByStep := vecsByStep(rhsRes.result, steps)

	type seriesAcc struct {
		labels  map[string]string
		samples []types.Sample
	}
	acc := make(map[string]*seriesAcc)
	numSteps := len(steps)

	for _, evalTimeMs := range steps {
		lhsVec := lhsVecByStep[evalTimeMs]
		rhsVec := rhsVecByStep[evalTimeMs]

		var resultVec types.Vector
		var binErr error
		switch plan.BinaryOp {
		case "and":
			resultVec = VectorAnd(lhsVec, rhsVec, vm)
		case "or":
			resultVec = VectorOr(lhsVec, rhsVec, vm)
		case "unless":
			resultVec = VectorUnless(lhsVec, rhsVec, vm)
		default:
			resultVec, binErr = VectorBinaryOp(lhsVec, rhsVec, plan.BinaryOp, vm, plan.ReturnBool)
		}
		if binErr != nil {
			return nil, binErr
		}

		for _, s := range resultVec {
			key := labelsKeyStr(s.Labels)
			a, ok := acc[key]
			if !ok {
				a = &seriesAcc{labels: s.Labels, samples: make([]types.Sample, 0, numSteps)}
				acc[key] = a
			}
			a.samples = append(a.samples, types.Sample{Timestamp: evalTimeMs, Value: s.F})
		}
	}

	if len(steps) == 1 {
		var vec types.Vector
		for _, a := range acc {
			for _, s := range a.samples {
				vec = append(vec, types.InstantSample{
					Labels:      a.labels,
					Fingerprint: fingerprint.Compute(a.labels),
					T:           s.Timestamp,
					F:           s.Value,
				})
			}
		}
		// Apply MathChain on binary result (e.g. exp(rate/1000))
		if len(plan.MathChain) > 0 {
			vec = applyMathChain(vec, plan.MathChain)
		}
		return &types.QueryResult{Type: "vector", Vector: vec}, nil
	}

	matrix := make(types.Matrix, 0, len(acc))
	for _, a := range acc {
		matrix = append(matrix, types.Series{
			Labels:      a.labels,
			Fingerprint: fingerprint.Compute(a.labels),
			Samples:     a.samples,
		})
	}
	// Apply MathChain on binary result (e.g. exp(rate/1000))
	if len(plan.MathChain) > 0 {
		matrix = applyMathChainMatrix(matrix, plan.MathChain)
	}
	return &types.QueryResult{Type: "matrix", Matrix: matrix}, nil
}

// vecsByStep extracts per-step vectors from a QueryResult.
func vecsByStep(qr *types.QueryResult, steps []int64) map[int64]types.Vector {
	result := make(map[int64]types.Vector, len(steps))
	if qr.Type == "vector" {
		for _, s := range qr.Vector {
			result[s.T] = append(result[s.T], s)
		}
	} else {
		for _, series := range qr.Matrix {
			for _, p := range series.Samples {
				result[p.Timestamp] = append(result[p.Timestamp], types.InstantSample{
					Labels:      series.Labels,
					Fingerprint: series.Fingerprint,
					T:           p.Timestamp,
					F:           p.Value,
				})
			}
		}
	}
	return result
}

func labelsKeyStr(m map[string]string) string {
	keys := sortedKeysMap(m)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(m[k])
		b.WriteByte(1)
	}
	return b.String()
}

// evalInstantSD evaluates expression for a single eval_time using SeriesData.
func (ev *Evaluator) evalInstantSD(
	seriesMap map[string]*clickhouse.SeriesData,
	plan *translator.SQLPlan,
	evalTimeMs, stalenessMs int64,
) types.Vector {

	// Handle absent(): check per-step whether any series has a value
	if plan.FuncName == "absent" || plan.ExprType == "absent" {
		// Check if any series has a value at this eval time
		hasValue := false
		for _, sd := range seriesMap {
			if _, ok := InstantValue(sd.Samples, evalTimeMs, stalenessMs); ok {
				hasValue = true
				break
			}
		}
		if !hasValue {
			// No value at this step → absent returns 1 with equality matcher labels
			ls := make(map[string]string)
			for _, m := range plan.AbsentMatchers {
				if m.Op == "=" && m.Name != "__name__" {
					ls[m.Name] = m.Val
				}
			}
			return types.Vector{{Labels: ls, Fingerprint: fingerprint.Compute(ls), F: 1.0, T: evalTimeMs}}
		}
		return types.Vector{} // series has value → absent returns nothing
	}

	var result types.Vector

	for _, sd := range seriesMap {
		val, ok := ev.computeValue(sd.Samples, plan, evalTimeMs, stalenessMs)
		if !ok {
			continue
		}
		fp := fingerprint.Compute(sd.Labels)
		result = append(result, types.InstantSample{
			Labels:      sd.Labels,
			Fingerprint: fp,
			T:           evalTimeMs,
			F:           val,
		})
	}

	if len(plan.AggChain) > 0 {
		result = applyAggChain(result, plan.AggChain)
	} else if plan.AggOp != "" {
		result = applyAggregation(result, plan)
	}

	// Math chain post-processing — applied AFTER aggregation
	if len(plan.MathChain) > 0 {
		result = applyMathChain(result, plan.MathChain)
	}

	// Histogram quantile post-processing
	if plan.ExprType == "histogram_quantile" || plan.FuncName == "histogram_quantile" {
		result = applyHistogramQuantile(result, plan.AggParam, evalTimeMs)
	}

	// scalar() post-processing: convert single-element vector to scalar
	if plan.ExprType == "scalar_func" {
		if len(result) == 1 {
			result = types.Vector{{
				Labels: map[string]string{},
				F:      result[0].F,
				T:      evalTimeMs,
			}}
		} else {
			result = types.Vector{{
				Labels: map[string]string{},
				F:      math.NaN(),
				T:      evalTimeMs,
			}}
		}
	}

	// label_replace / label_join post-processing
	if plan.FuncName == "label_replace" && len(plan.LabelFuncArgs) >= 4 {
		if replaced, err := EvalLabelReplace(result, plan.LabelFuncArgs[0], plan.LabelFuncArgs[1], plan.LabelFuncArgs[2], plan.LabelFuncArgs[3]); err == nil {
			result = replaced
		}
	}
	if plan.FuncName == "label_join" && len(plan.LabelFuncArgs) >= 2 {
		result = EvalLabelJoin(result, plan.LabelFuncArgs[0], plan.LabelFuncArgs[1], plan.LabelFuncArgs[2:])
	}

	// Deterministic order (unless MathChain contains sort)
	hasSortInChain := false
	for _, m := range plan.MathChain {
		if m.Fn == "sort" || m.Fn == "sort_desc" || m.Fn == "sort_by_label" || m.Fn == "sort_by_label_desc" {
			hasSortInChain = true
			break
		}
	}
	if !hasSortInChain {
		slices.SortFunc(result, func(a, b types.InstantSample) int {
			return cmp.Compare(a.Fingerprint, b.Fingerprint)
		})
	}
	return result
}

// buildMatrixSD iterates series-first (better cache locality) then steps.
// Fingerprint computed once per series, not per step.
func (ev *Evaluator) buildMatrixSD(
	seriesMap map[string]*clickhouse.SeriesData,
	plan *translator.SQLPlan,
	steps []int64,
	stalenessMs int64,
) types.Matrix {
	// Check if we need per-step aggregation
	hasAgg := len(plan.AggChain) > 0 || plan.AggOp != ""
	hasHistogram := plan.ExprType == "histogram_quantile" || plan.FuncName == "histogram_quantile"
	hasMathChain := len(plan.MathChain) > 0
	hasLabelFunc := plan.FuncName == "label_replace" || plan.FuncName == "label_join"
	hasAbsent := plan.FuncName == "absent" || plan.ExprType == "absent"
	hasScalarFunc := plan.ExprType == "scalar_func"

	// For complex plans that need per-step processing, fall back to step-first
	if hasAbsent || hasScalarFunc || hasHistogram {
		return ev.buildMatrixSD_stepFirst(seriesMap, plan, steps, stalenessMs)
	}

	// Series-first: compute all steps for each series, then aggregate
	numSteps := len(steps)
	matrix := make(types.Matrix, 0, len(seriesMap))

	// Adjust eval times for offset once
	evalPlan := plan
	offsetMs := plan.OffsetMs

	for _, sd := range seriesMap {
		fp := fingerprint.Compute(sd.Labels)
		samples := make([]types.Sample, 0, numSteps)

		for _, evalTimeMs := range steps {
			adjEvalTime := evalTimeMs
			if offsetMs != 0 {
				adjEvalTime -= offsetMs
			}
			val, ok := ev.computeValueNoOffset(sd.Samples, evalPlan, adjEvalTime, stalenessMs)
			if !ok {
				continue
			}
			samples = append(samples, types.Sample{Timestamp: evalTimeMs, Value: val})
		}
		if len(samples) > 0 {
			matrix = append(matrix, types.Series{
				Labels:      sd.Labels,
				Fingerprint: fp,
				Samples:     samples,
			})
		}
	}

	// Apply aggregation chain on the matrix if needed
	if hasAgg {
		matrix = ev.aggregateMatrix(matrix, plan, steps)
	}

	// Apply math chain post-aggregation
	if hasMathChain {
		matrix = applyMathChainMatrix(matrix, plan.MathChain)
	}

	// Apply label functions
	if hasLabelFunc && plan.FuncName == "label_replace" && len(plan.LabelFuncArgs) >= 4 {
		for i := range matrix {
			vec := types.Vector{{Labels: matrix[i].Labels, F: 0}}
			if replaced, err := EvalLabelReplace(vec, plan.LabelFuncArgs[0], plan.LabelFuncArgs[1], plan.LabelFuncArgs[2], plan.LabelFuncArgs[3]); err == nil && len(replaced) > 0 {
				matrix[i].Labels = replaced[0].Labels
				matrix[i].Fingerprint = fingerprint.Compute(replaced[0].Labels)
			}
		}
	}

	slices.SortFunc(matrix, func(a, b types.Series) int {
		return cmp.Compare(a.Fingerprint, b.Fingerprint)
	})
	return matrix
}

// computeValueNoOffset is like computeValue but assumes offset is already applied to evalTimeMs.
func (ev *Evaluator) computeValueNoOffset(
	samples []types.Sample,
	plan *translator.SQLPlan,
	evalTimeMs, stalenessMs int64,
) (float64, bool) {
	rangeMs := plan.RangeMs

	switch plan.FuncName {
	case "rate":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, true, true)
	case "irate":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return IRate(w)
	case "increase":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, true, false)
	case "delta":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, false, false)
	case "idelta":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return IDelta(w)
	case "deriv":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return Deriv(w)
	case "predict_linear":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return PredictLinear(w, evalTimeMs, plan.AggParam)
	case "resets":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		if len(w) == 0 {
			return 0, false
		}
		return Resets(w), true
	case "changes":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		if len(w) == 0 {
			return 0, false
		}
		return Changes(w), true
	case "avg_over_time":
		return AvgOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "min_over_time":
		return MinOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "max_over_time":
		return MaxOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "sum_over_time":
		return SumOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "count_over_time":
		return CountOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "stddev_over_time":
		return StddevOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "stdvar_over_time":
		return StdvarOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "last_over_time":
		return LastOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "present_over_time":
		return PresentOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "quantile_over_time":
		return QuantileOverTime(plan.AggParam, WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "mad_over_time":
		return MadOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "double_exponential_smoothing":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		v, err := DoubleExponentialSmoothing(w, plan.AggParam, plan.SmoothingTF)
		if err != nil {
			return 0, false
		}
		return v, true
	case "histogram_quantile":
		inner := plan.InnerFuncName
		if inner == "" {
			inner = "rate"
		}
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		switch inner {
		case "rate":
			return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, true, true)
		case "increase":
			return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, true, false)
		default:
			return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, true, true)
		}
	default:
		return InstantValue(samples, evalTimeMs, stalenessMs)
	}
}

// aggregateMatrix aggregates a matrix per-step using plan's aggregation settings.
// Pre-computes group keys per series to avoid 803K MatchingKey calls.
func (ev *Evaluator) aggregateMatrix(matrix types.Matrix, plan *translator.SQLPlan, steps []int64) types.Matrix {
	// Get aggregation params — use first AggChain step or legacy fields
	var grouping []string
	var without bool
	var aggOp string
	if len(plan.AggChain) > 0 {
		// For single-level simple agg, optimize directly
		// For multi-level or complex ops, fall back to per-step applyAggChain
		if len(plan.AggChain) > 1 {
			return ev.aggregateMatrixSlow(matrix, plan)
		}
		step := plan.AggChain[0]
		aggOp = step.Op
		grouping = step.Grouping
		without = step.Without
	} else {
		aggOp = plan.AggOp
		grouping = plan.Grouping
		without = plan.Without
	}

	// Pre-compute group key + group labels per series (computed ONCE, not per step)
	metas := make([]seriesMeta, len(matrix))
	for i, s := range matrix {
		metas[i] = seriesMeta{
			groupKey:    MatchingKey(s.Labels, !without, grouping),
			groupLabels: groupLabels(s.Labels, grouping, without),
		}
	}

	// Collect timestamps
	tsSet := make(map[int64]struct{}, len(steps))
	for _, s := range matrix {
		for _, p := range s.Samples {
			tsSet[p.Timestamp] = struct{}{}
		}
	}
	timestamps := make([]int64, 0, len(tsSet))
	for ts := range tsSet {
		timestamps = append(timestamps, ts)
	}
	slices.Sort(timestamps)

	// For simple ops (sum/avg/min/max/count/group), aggregate inline without building vector
	switch aggOp {
	case "sum", "avg", "min", "max", "count", "group":
		return ev.aggregateMatrixSimple(matrix, metas, timestamps, aggOp)
	}

	// For complex ops (topk, bottomk, quantile, count_values, etc.), fall back to per-step
	return ev.aggregateMatrixSlow(matrix, plan)
}

// aggregateMatrixSimple handles sum/avg/min/max/count/group with pre-computed keys.
// Zero MatchingKey calls per step — keys computed once in aggregateMatrix.
func (ev *Evaluator) aggregateMatrixSimple(
	matrix types.Matrix,
	metas []seriesMeta,
	timestamps []int64,
	op string,
) types.Matrix {
	type group struct {
		labels  map[string]string
		samples map[int64]float64 // ts → aggregated value
		counts  map[int64]int     // ts → count (for avg)
	}
	groups := make(map[string]*group)
	var order []string

	cursors := make([]int, len(matrix))

	for _, ts := range timestamps {
		for i := range matrix {
			s := &matrix[i]
			c := cursors[i]
			for c < len(s.Samples) && s.Samples[c].Timestamp < ts {
				c++
			}
			if c >= len(s.Samples) || s.Samples[c].Timestamp != ts {
				cursors[i] = c
				continue
			}
			val := s.Samples[c].Value
			c++
			cursors[i] = c

			key := metas[i].groupKey
			g, ok := groups[key]
			if !ok {
				g = &group{
					labels:  metas[i].groupLabels,
					samples: make(map[int64]float64, len(timestamps)),
					counts:  make(map[int64]int, len(timestamps)),
				}
				groups[key] = g
				order = append(order, key)
			}

			cnt := g.counts[ts]
			switch op {
			case "sum":
				g.samples[ts] += val
			case "avg":
				// Online mean: mean += (val - mean) / n
				n := float64(cnt + 1)
				g.samples[ts] += (val - g.samples[ts]) / n
			case "min":
				if cnt == 0 || val < g.samples[ts] {
					g.samples[ts] = val
				}
			case "max":
				if cnt == 0 || val > g.samples[ts] {
					g.samples[ts] = val
				}
			case "count":
				g.samples[ts] = float64(cnt + 1)
			case "group":
				g.samples[ts] = 1.0
			}
			g.counts[ts] = cnt + 1
		}
	}

	result := make(types.Matrix, 0, len(groups))
	for _, key := range order {
		g := groups[key]
		samples := make([]types.Sample, 0, len(timestamps))
		for _, ts := range timestamps {
			if _, ok := g.samples[ts]; ok {
				samples = append(samples, types.Sample{Timestamp: ts, Value: g.samples[ts]})
			}
		}
		result = append(result, types.Series{
			Labels:      g.labels,
			Fingerprint: fingerprint.Compute(g.labels),
			Samples:     samples,
		})
	}
	return result
}

// aggregateMatrixTopK handles topk/bottomk with pre-computed keys.
func (ev *Evaluator) aggregateMatrixTopK(
	matrix types.Matrix,
	metas []seriesMeta,
	timestamps []int64,
	k int,
	bottom bool,
) types.Matrix {
	// For topk/bottomk, we keep original labels (not grouped).
	// Sort by last value, take top k.
	type lastVal struct {
		idx int
		val float64
	}
	vals := make([]lastVal, len(matrix))
	for i, s := range matrix {
		v := 0.0
		if len(s.Samples) > 0 {
			v = s.Samples[len(s.Samples)-1].Value
		}
		vals[i] = lastVal{idx: i, val: v}
	}
	slices.SortFunc(vals, func(a, b lastVal) int {
		if bottom {
			return cmp.Compare(a.val, b.val)
		}
		return cmp.Compare(b.val, a.val)
	})
	if k > len(vals) {
		k = len(vals)
	}
	result := make(types.Matrix, k)
	for i := 0; i < k; i++ {
		result[i] = matrix[vals[i].idx]
	}
	return result
}

// aggregateMatrixSlow falls back to per-step vector aggregation for complex ops.
func (ev *Evaluator) aggregateMatrixSlow(matrix types.Matrix, plan *translator.SQLPlan) types.Matrix {
	tsSet := make(map[int64]struct{})
	for _, s := range matrix {
		for _, p := range s.Samples {
			tsSet[p.Timestamp] = struct{}{}
		}
	}
	timestamps := make([]int64, 0, len(tsSet))
	for ts := range tsSet {
		timestamps = append(timestamps, ts)
	}
	slices.Sort(timestamps)

	cursors := make([]int, len(matrix))
	type seriesAcc struct {
		labels  map[string]string
		fp      uint64
		samples []types.Sample
	}
	acc := make(map[uint64]*seriesAcc)
	vec := make(types.Vector, 0, len(matrix))

	for _, ts := range timestamps {
		vec = vec[:0]
		for i := range matrix {
			s := &matrix[i]
			c := cursors[i]
			for c < len(s.Samples) && s.Samples[c].Timestamp < ts {
				c++
			}
			if c < len(s.Samples) && s.Samples[c].Timestamp == ts {
				vec = append(vec, types.InstantSample{
					Labels: s.Labels, Fingerprint: s.Fingerprint, T: ts, F: s.Samples[c].Value,
				})
				c++
			}
			cursors[i] = c
		}
		if len(plan.AggChain) > 0 {
			vec = applyAggChain(vec, plan.AggChain)
		} else if plan.AggOp != "" {
			vec = applyAggregation(vec, plan)
		}
		for _, s := range vec {
			a, ok := acc[s.Fingerprint]
			if !ok {
				a = &seriesAcc{labels: s.Labels, fp: s.Fingerprint, samples: make([]types.Sample, 0, len(timestamps))}
				acc[s.Fingerprint] = a
			}
			a.samples = append(a.samples, types.Sample{Timestamp: ts, Value: s.F})
		}
	}

	result := make(types.Matrix, 0, len(acc))
	for _, a := range acc {
		result = append(result, types.Series{Labels: a.labels, Fingerprint: a.fp, Samples: a.samples})
	}
	return result
}

// applyMathChainResult applies MathChain to a QueryResult (vector or matrix).
func applyMathChainResult(qr *types.QueryResult, chain []translator.MathStep) {
	if qr.Type == "vector" {
		qr.Vector = applyMathChain(qr.Vector, chain)
	} else if qr.Type == "matrix" {
		qr.Matrix = applyMathChainMatrix(qr.Matrix, chain)
	}
}

type seriesMeta struct {
	groupKey    string
	groupLabels map[string]string
}

// applyMathChainMatrix applies math transformations to all samples in a matrix.
func applyMathChainMatrix(matrix types.Matrix, chain []translator.MathStep) types.Matrix {
	for _, step := range chain {
		if step.Fn == "sort" || step.Fn == "sort_desc" {
			// Sort matrix by last value
			slices.SortFunc(matrix, func(a, b types.Series) int {
				va, vb := lastSampleValue(a), lastSampleValue(b)
				if step.Fn == "sort_desc" {
					return cmp.Compare(vb, va)
				}
				return cmp.Compare(va, vb)
			})
			continue
		}
		for i := range matrix {
			for j := range matrix[i].Samples {
				matrix[i].Samples[j].Value = applyMathOp(step, matrix[i].Samples[j].Value)
			}
		}
	}
	return matrix
}

func lastSampleValue(s types.Series) float64 {
	if len(s.Samples) == 0 {
		return 0
	}
	return s.Samples[len(s.Samples)-1].Value
}

func applyMathOp(step translator.MathStep, v float64) float64 {
	switch step.Fn {
	case "abs":
		return math.Abs(v)
	case "ceil":
		return math.Ceil(v)
	case "floor":
		return math.Floor(v)
	case "sqrt":
		return math.Sqrt(v)
	case "exp":
		return math.Exp(v)
	case "ln":
		return math.Log(v)
	case "log2":
		return math.Log2(v)
	case "log10":
		return math.Log10(v)
	case "sgn":
		if v > 0 {
			return 1
		} else if v < 0 {
			return -1
		}
		return 0
	case "clamp_min":
		return math.Max(step.Param, v)
	case "clamp_max":
		return math.Min(step.Param, v)
	case "clamp":
		return math.Max(step.Param, math.Min(step.Param2, v))
	case "round":
		if step.Param == 0 || step.Param == 1 {
			return math.Round(v)
		}
		return math.Round(v/step.Param) * step.Param
	default:
		return v
	}
}

// buildMatrixSD_stepFirst is the fallback for complex plans (absent, scalar, histogram).
func (ev *Evaluator) buildMatrixSD_stepFirst(
	seriesMap map[string]*clickhouse.SeriesData,
	plan *translator.SQLPlan,
	steps []int64,
	stalenessMs int64,
) types.Matrix {
	type seriesAcc struct {
		labels  map[string]string
		fp      uint64
		samples []types.Sample
	}
	acc := make(map[uint64]*seriesAcc, len(seriesMap))
	numSteps := len(steps)

	for _, evalTimeMs := range steps {
		vec := ev.evalInstantSD(seriesMap, plan, evalTimeMs, stalenessMs)
		for _, s := range vec {
			a, ok := acc[s.Fingerprint]
			if !ok {
				a = &seriesAcc{labels: s.Labels, fp: s.Fingerprint, samples: make([]types.Sample, 0, numSteps)}
				acc[s.Fingerprint] = a
			}
			a.samples = append(a.samples, types.Sample{Timestamp: evalTimeMs, Value: s.F})
		}
	}

	matrix := make(types.Matrix, 0, len(acc))
	for _, a := range acc {
		matrix = append(matrix, types.Series{Labels: a.labels, Fingerprint: a.fp, Samples: a.samples})
	}
	slices.SortFunc(matrix, func(a, b types.Series) int {
		return cmp.Compare(a.Fingerprint, b.Fingerprint)
	})
	return matrix
}

// computeValue computes the value for a single series and eval_time.
func (ev *Evaluator) computeValue(
	samples []types.Sample,
	plan *translator.SQLPlan,
	evalTimeMs, stalenessMs int64,
) (float64, bool) {

	// Adjust eval time for offset: data was fetched shifted, so window lookups
	// need to use the offset-adjusted eval time to match sample timestamps.
	if plan.OffsetMs != 0 {
		evalTimeMs = evalTimeMs - plan.OffsetMs
	}

	rangeMs := plan.RangeMs

	switch plan.FuncName {
	case "rate":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, true, true)
	case "irate":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return IRate(w)
	case "increase":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, true, false)
	case "delta":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, false, false)
	case "idelta":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return IDelta(w)
	case "deriv":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return Deriv(w)
	case "predict_linear":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		return PredictLinear(w, evalTimeMs, plan.AggParam)
	case "resets":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		if len(w) == 0 {
			return 0, false
		}
		return Resets(w), true
	case "changes":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		if len(w) == 0 {
			return 0, false
		}
		return Changes(w), true

	// Over-time functions
	case "avg_over_time":
		return AvgOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "min_over_time":
		return MinOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "max_over_time":
		return MaxOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "sum_over_time":
		return SumOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "count_over_time":
		return CountOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "stddev_over_time":
		return StddevOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "stdvar_over_time":
		return StdvarOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "last_over_time":
		return LastOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "present_over_time":
		return PresentOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "quantile_over_time":
		return QuantileOverTime(plan.AggParam, WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "mad_over_time":
		return MadOverTime(WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs))
	case "double_exponential_smoothing":
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		v, err := DoubleExponentialSmoothing(w, plan.AggParam, plan.SmoothingTF)
		if err != nil {
			return 0, false
		}
		return v, true

	// histogram_quantile: compute inner function (rate/increase) per bucket,
	// then applyHistogramQuantile groups and interpolates in evalInstantSD.
	case "histogram_quantile":
		inner := plan.InnerFuncName
		if inner == "" {
			inner = "rate" // default inner function for histogram
		}
		w := WindowSamples(samples, evalTimeMs-rangeMs, evalTimeMs)
		switch inner {
		case "rate":
			return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, true, true)
		case "increase":
			return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, true, false)
		case "irate":
			return IRate(w)
		default:
			return ExtrapolatedRate(w, evalTimeMs-rangeMs, evalTimeMs, true, true)
		}

	// Math functions are NO LONGER in computeValue.
	// They are applied as post-processing in evalInstantSD AFTER applyAggChain.
	// See plan.MathChain and applyMathChain().

	default:
		return InstantValue(samples, evalTimeMs, stalenessMs)
	}
}

// innerValue evaluates the inner function if present, otherwise does InstantValue.
func (ev *Evaluator) innerValue(samples []types.Sample, plan *translator.SQLPlan, evalTimeMs, stalenessMs int64) (float64, bool) {
	if plan.Inner != nil {
		return ev.computeValue(samples, plan.Inner, evalTimeMs, stalenessMs)
	}
	return InstantValue(samples, evalTimeMs, stalenessMs)
}

func generateSteps(start, end time.Time, step time.Duration) []int64 {
	if step <= 0 {
		return []int64{end.UnixMilli()}
	}
	d := end.Sub(start)
	if d < 0 {
		return []int64{end.UnixMilli()}
	}
	n := int(d/step) + 1
	steps := make([]int64, 0, n)
	for t := start; !t.After(end); t = t.Add(step) {
		steps = append(steps, t.UnixMilli())
	}
	if len(steps) == 0 {
		steps = []int64{end.UnixMilli()}
	}
	return steps
}

// TooManySeriesError is returned when too many series are fetched.
type TooManySeriesError struct {
	Count, Max int
}

func (e *TooManySeriesError) Error() string {
	return fmt.Sprintf("too many series: %d (max %d)", e.Count, e.Max)
}

func applyAggregation(vec types.Vector, plan *translator.SQLPlan) types.Vector {
	var result types.Vector

	switch plan.AggOp {
	case "sum":
		result = AggregateVector(vec, "sum", plan.Grouping, plan.Without)
	case "avg":
		result = AggregateVector(vec, "avg", plan.Grouping, plan.Without)
	case "min":
		result = AggregateVector(vec, "min", plan.Grouping, plan.Without)
	case "max":
		result = AggregateVector(vec, "max", plan.Grouping, plan.Without)
	case "count":
		result = AggregateVector(vec, "count", plan.Grouping, plan.Without)
	case "stddev":
		result = AggregateVector(vec, "stddev", plan.Grouping, plan.Without)
	case "stdvar":
		result = AggregateVector(vec, "stdvar", plan.Grouping, plan.Without)
	case "group":
		result = AggregateVector(vec, "group", plan.Grouping, plan.Without)
	case "topk":
		result = AggregateTopK(int(plan.AggParam), vec, plan.Grouping, plan.Without, false)
	case "bottomk":
		result = AggregateTopK(int(plan.AggParam), vec, plan.Grouping, plan.Without, true)
	case "quantile":
		result = aggregateQuantile(plan.AggParam, vec, plan.Grouping, plan.Without)
	case "count_values":
		result = AggregateCountValues(plan.AggLabel, vec, plan.Grouping, plan.Without)
	case "limitk":
		result = Limitk(int(plan.AggParam), vec)
	case "limit_ratio":
		r, _ := LimitRatio(plan.AggParam, vec)
		result = r
	default:
		result = vec
	}

	return result
}

// applyAggChain applies a chain of aggregation steps, innermost first.
// applyMathChain applies math wrapper functions to all samples in the vector.
// Applied AFTER aggregation — e.g. clamp_min(sum(rate(...)), 30000).
func applyMathChain(vec types.Vector, chain []translator.MathStep) types.Vector {
	for _, step := range chain {
		switch step.Fn {
		case "abs":
			for i := range vec { vec[i].F = math.Abs(vec[i].F) }
		case "ceil":
			for i := range vec { vec[i].F = math.Ceil(vec[i].F) }
		case "floor":
			for i := range vec { vec[i].F = math.Floor(vec[i].F) }
		case "round":
			for i := range vec {
				if step.Param == 0 || step.Param == 1 {
					vec[i].F = math.Round(vec[i].F)
				} else {
					vec[i].F = math.Round(vec[i].F/step.Param) * step.Param
				}
			}
		case "sqrt":
			for i := range vec { vec[i].F = math.Sqrt(vec[i].F) }
		case "exp":
			for i := range vec { vec[i].F = math.Exp(vec[i].F) }
		case "ln":
			for i := range vec { vec[i].F = math.Log(vec[i].F) }
		case "log2":
			for i := range vec { vec[i].F = math.Log2(vec[i].F) }
		case "log10":
			for i := range vec { vec[i].F = math.Log10(vec[i].F) }
		case "sgn":
			for i := range vec {
				if vec[i].F > 0 { vec[i].F = 1 } else if vec[i].F < 0 { vec[i].F = -1 } else { vec[i].F = 0 }
			}
		case "clamp_min":
			for i := range vec { vec[i].F = math.Max(step.Param, vec[i].F) }
		case "clamp_max":
			for i := range vec { vec[i].F = math.Min(step.Param, vec[i].F) }
		case "clamp":
			for i := range vec { vec[i].F = math.Max(step.Param, math.Min(step.Param2, vec[i].F)) }
		case "sin":
			for i := range vec { vec[i].F = math.Sin(vec[i].F) }
		case "cos":
			for i := range vec { vec[i].F = math.Cos(vec[i].F) }
		case "tan":
			for i := range vec { vec[i].F = math.Tan(vec[i].F) }
		case "asin":
			for i := range vec { vec[i].F = math.Asin(vec[i].F) }
		case "acos":
			for i := range vec { vec[i].F = math.Acos(vec[i].F) }
		case "atan":
			for i := range vec { vec[i].F = math.Atan(vec[i].F) }
		case "sinh":
			for i := range vec { vec[i].F = math.Sinh(vec[i].F) }
		case "cosh":
			for i := range vec { vec[i].F = math.Cosh(vec[i].F) }
		case "tanh":
			for i := range vec { vec[i].F = math.Tanh(vec[i].F) }
		case "asinh":
			for i := range vec { vec[i].F = math.Asinh(vec[i].F) }
		case "acosh":
			for i := range vec { vec[i].F = math.Acosh(vec[i].F) }
		case "atanh":
			for i := range vec { vec[i].F = math.Atanh(vec[i].F) }
		case "deg":
			for i := range vec { vec[i].F = vec[i].F * 180 / math.Pi }
		case "rad":
			for i := range vec { vec[i].F = vec[i].F * math.Pi / 180 }
		case "sort":
			slices.SortFunc(vec, func(a, b types.InstantSample) int { return cmp.Compare(a.F, b.F) })
		case "sort_desc":
			slices.SortFunc(vec, func(a, b types.InstantSample) int { return cmp.Compare(b.F, a.F) })
		case "sort_by_label":
			vec = SortByLabel(vec, step.SortLabels...)
		case "sort_by_label_desc":
			vec = SortByLabelDesc(vec, step.SortLabels...)
		}
	}
	return vec
}

func applyAggChain(vec types.Vector, chain []translator.AggStep) types.Vector {
	result := vec
	for _, step := range chain {
		result = applyOneAggStep(result, step)
	}
	return result
}

func applyOneAggStep(vec types.Vector, step translator.AggStep) types.Vector {
	switch step.Op {
	case "sum", "avg", "min", "max", "count", "stddev", "stdvar", "group":
		return AggregateVector(vec, step.Op, step.Grouping, step.Without)
	case "topk":
		return AggregateTopK(int(step.Param), vec, step.Grouping, step.Without, false)
	case "bottomk":
		return AggregateTopK(int(step.Param), vec, step.Grouping, step.Without, true)
	case "quantile":
		return aggregateQuantile(step.Param, vec, step.Grouping, step.Without)
	case "count_values":
		return AggregateCountValues(step.Label, vec, step.Grouping, step.Without)
	case "limitk":
		return Limitk(int(step.Param), vec)
	case "limit_ratio":
		r, _ := LimitRatio(step.Param, vec)
		return r
	}
	return vec
}

// applyHistogramQuantile groups vector by all labels except "le" and computes quantile.
// evalTimeMs is passed through so output samples have the correct timestamp.
func applyHistogramQuantile(vec types.Vector, q float64, evalTimeMs int64) types.Vector {
	type group struct {
		labels  map[string]string
		buckets []Bucket
	}
	groups := make(map[string]*group)
	var order []string

	for _, s := range vec {
		groupLabels := make(map[string]string)
		for k, v := range s.Labels {
			if k != "le" {
				groupLabels[k] = v
			}
		}
		key := labelsKeyStr(groupLabels)

		g, ok := groups[key]
		if !ok {
			g = &group{labels: groupLabels}
			groups[key] = g
			order = append(order, key)
		}

		leStr := s.Labels["le"]
		var le float64
		if leStr == "+Inf" {
			le = math.Inf(1)
		} else {
			le, _ = strconv.ParseFloat(leStr, 64)
		}
		g.buckets = append(g.buckets, Bucket{UpperBound: le, Count: s.F})
	}

	var result types.Vector
	for _, key := range order {
		g := groups[key]
		val, _, ok := HistogramQuantile(q, g.buckets)
		if !ok {
			continue
		}
		result = append(result, types.InstantSample{
			Labels:      g.labels,
			Fingerprint: fingerprint.Compute(g.labels),
			F:           val,
			T:           evalTimeMs,
		})
	}
	return result
}

// scalarBinaryOpVector applies a binary op between each sample in vec and a scalar.
func scalarBinaryOpVector(vec types.Vector, scalar float64, op string, returnBool bool, scalarOnLeft bool) types.Vector {
	var result types.Vector
	for _, s := range vec {
		var lv, rv float64
		if scalarOnLeft {
			lv, rv = scalar, s.F
		} else {
			lv, rv = s.F, scalar
		}
		val, keep := ApplyBinaryOp(lv, rv, op)
		if !keep && !returnBool {
			continue
		}
		if returnBool {
			if keep {
				val = 1
			} else {
				val = 0
			}
		}
		result = append(result, types.InstantSample{
			Labels:      s.Labels,
			Fingerprint: s.Fingerprint,
			T:           s.T,
			F:           val,
		})
	}
	return result
}

// applyScalarBinary applies a scalar binary op to a QueryResult.
func applyScalarBinary(qr *types.QueryResult, scalar float64, op string, returnBool bool, scalarOnLeft bool) *types.QueryResult {
	if qr.Type == "vector" {
		vec := scalarBinaryOpVector(qr.Vector, scalar, op, returnBool, scalarOnLeft)
		return &types.QueryResult{Type: "vector", Vector: vec}
	}
	// Matrix: apply per series
	var newMatrix types.Matrix
	for _, series := range qr.Matrix {
		var newSamples []types.Sample
		for _, s := range series.Samples {
			var lv, rv float64
			if scalarOnLeft {
				lv, rv = scalar, s.Value
			} else {
				lv, rv = s.Value, scalar
			}
			val, keep := ApplyBinaryOp(lv, rv, op)
			if !keep && !returnBool {
				continue
			}
			if returnBool {
				if keep {
					val = 1
				} else {
					val = 0
				}
			}
			newSamples = append(newSamples, types.Sample{Timestamp: s.Timestamp, Value: val})
		}
		if len(newSamples) > 0 {
			newMatrix = append(newMatrix, types.Series{Labels: series.Labels, Fingerprint: series.Fingerprint, Samples: newSamples})
		}
	}
	return &types.QueryResult{Type: "matrix", Matrix: newMatrix}
}

// isScalarResult checks if a QueryResult represents a scalar (single element with empty labels).
func isScalarResult(qr *types.QueryResult) bool {
	if qr.Type == "vector" && len(qr.Vector) == 1 && len(qr.Vector[0].Labels) == 0 {
		return true
	}
	if qr.Type == "matrix" && len(qr.Matrix) == 1 && len(qr.Matrix[0].Labels) == 0 {
		return true
	}
	return false
}

// extractScalarValue gets the scalar float from a scalar result.
func extractScalarValue(qr *types.QueryResult) float64 {
	if qr.Type == "vector" && len(qr.Vector) > 0 {
		return qr.Vector[0].F
	}
	if qr.Type == "matrix" && len(qr.Matrix) > 0 && len(qr.Matrix[0].Samples) > 0 {
		return qr.Matrix[0].Samples[0].Value
	}
	return math.NaN()
}

// isMVCompatible checks if a plan can use a materialized view tier.
func isMVCompatible(plan *translator.SQLPlan, tier *config.DownsampleTier) bool {
	// Counter functions require raw samples
	incompatible := map[string]bool{
		"rate": true, "irate": true, "increase": true,
		"deriv": true, "predict_linear": true,
		"resets": true, "changes": true,
		"delta": true, "idelta": true,
		"histogram_quantile": true,
	}
	if incompatible[plan.FuncName] {
		return false
	}
	// Check if tier supports this function
	for _, f := range tier.SupportedFuncs {
		if f == plan.FuncName {
			return true
		}
	}
	// Plain vector selector can use last_val
	return plan.FuncName == ""
}
