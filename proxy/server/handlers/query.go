package handlers

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hinskii/promclick/config"
	"github.com/hinskii/promclick/eval"
	"github.com/hinskii/promclick/fingerprint"
	"github.com/hinskii/promclick/translator"
	"github.com/hinskii/promclick/types"

	proxycfg "github.com/hinskii/promclick/proxy/config"
	nativech "github.com/hinskii/promclick/proxy/clickhouse"
)

// Handler holds shared dependencies for query handlers.
type Handler struct {
	Cfg       *proxycfg.Config
	PromCfg   *config.Config
	Evaluator *eval.Evaluator
	Meta      *MetaQuerier
	Writer    *nativech.Writer
	Pool      *nativech.Pool // native TCP pool for downsampled queries
}

// Query handles /api/v1/query (instant query).
func (h *Handler) Query(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

	query := formOrQuery(r, "query")
	if query == "" {
		writeError(w, http.StatusBadRequest, "bad_data", "missing required parameter: query")
		return
	}

	evalTime, err := parsePrometheusTime(formOrQuery(r, "time"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid time: %v", err))
		return
	}

	t0 := time.Now()

	step := h.PromCfg.Prometheus.DefaultStep
	lookback := time.Duration(h.PromCfg.Prometheus.StalenessSeconds) * time.Second
	start := evalTime.Add(-lookback)

	transpiler := translator.New(h.PromCfg, start, evalTime, step)
	plan, err := transpiler.TranspileQuery(query)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "bad_data", fmt.Sprintf("parse error: %v", err))
		return
	}
	slog.Debug("transpile", "duration", time.Since(t0), "query", query)

	// Label cache fast-path for instant queries
	if result, ok := h.tryCacheOnlyAgg(plan, evalTime, evalTime, 0, t0); ok {
		writeJSON(w, APIResponse{Status: "success", Data: formatResult(result)})
		return
	}

	t1 := time.Now()
	result, err := h.Evaluator.EvalPlan(r.Context(), plan, evalTime, evalTime, 0)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "execution", fmt.Sprintf("%v", err))
		return
	}

	series := resultSeriesCount(result)
	slog.Debug("eval", "duration", time.Since(t1), "series", series)

	slog.Info("query",
		"query", query,
		"series", series,
		"total", time.Since(t0),
	)

	writeJSON(w, APIResponse{
		Status: "success",
		Data:   formatResult(result),
	})
}

// QueryRange handles /api/v1/query_range (range query).
func (h *Handler) QueryRange(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

	query := formOrQuery(r, "query")
	if query == "" {
		writeError(w, http.StatusBadRequest, "bad_data", "missing required parameter: query")
		return
	}

	start, err := parsePrometheusTime(formOrQuery(r, "start"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid start: %v", err))
		return
	}

	end, err := parsePrometheusTime(formOrQuery(r, "end"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid end: %v", err))
		return
	}

	stepStr := formOrQuery(r, "step")
	if stepStr == "" {
		writeError(w, http.StatusBadRequest, "bad_data", "missing required parameter: step")
		return
	}
	step, err := parsePrometheusDuration(stepStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid step: %v", err))
		return
	}
	if step <= 0 {
		writeError(w, http.StatusBadRequest, "bad_data", "zero or negative query resolution step widths are not accepted")
		return
	}

	// Prometheus default: max 11000 data points per query
	const maxPoints = 11000
	numPoints := int64(end.Sub(start) / step)
	if numPoints > maxPoints {
		writeError(w, http.StatusBadRequest, "bad_data",
			fmt.Sprintf("exceeded maximum resolution of %d points per timeseries. Try decreasing the query resolution (?step=XX)", maxPoints))
		return
	}

	t0 := time.Now()

	transpiler := translator.New(h.PromCfg, start, end, step)
	plan, err := transpiler.TranspileQuery(query)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "bad_data", fmt.Sprintf("parse error: %v", err))
		return
	}
	slog.Debug("transpile", "duration", time.Since(t0), "query", query)

	// Label cache fast-path: count/group aggregations on plain selectors
	// can be answered from cached labels without fetching samples from CH.
	if result, ok := h.tryCacheOnlyAgg(plan, start, end, step, t0); ok {
		writeResult(w, result)
		return
	}

	// Downsampling fast-path: try tier query for supported functions
	if h.Pool != nil && h.Cfg.Downsampling.Enabled && plan.FuncName != "" && plan.MetricName != "" {
		if plan.FuncName == "histogram_quantile" {
			if result, ok := h.tryDownsampledHistogram(r, plan, start, end, step, t0); ok {
				writeResult(w, result)
				return
			}
		} else if result, ok := h.tryDownsampledQuery(r, plan, start, end, step, t0); ok {
			writeResult(w, result)
			return
		}
	}

	t1 := time.Now()
	result, err := h.Evaluator.EvalPlan(r.Context(), plan, start, end, step)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "execution", fmt.Sprintf("%v", err))
		return
	}

	series := resultSeriesCount(result)
	slog.Debug("eval", "duration", time.Since(t1), "series", series)

	slog.Info("query_range",
		"query", query,
		"range", end.Sub(start).String(),
		"step", step.String(),
		"series", series,
		"total", time.Since(t0),
	)

	writeResult(w, result)
}

// writeResult writes a query result as a Prometheus API JSON response.
// Uses streaming for matrix results to avoid O(datapoints) allocations.
func writeResult(w http.ResponseWriter, qr *types.QueryResult) {
	if qr.Type == "matrix" && len(qr.Matrix) > 0 {
		writeMatrixResponse(w, qr.Matrix)
		return
	}
	writeJSON(w, APIResponse{
		Status: "success",
		Data:   formatResult(qr),
	})
}

// formatResult converts a QueryResult to the Prometheus API format.
func formatResult(qr *types.QueryResult) interface{} {
	switch qr.Type {
	case "vector":
		return formatVector(qr.Vector)
	case "matrix":
		return formatMatrix(qr.Matrix)
	default:
		return map[string]interface{}{
			"resultType": qr.Type,
			"result":     []interface{}{},
		}
	}
}

func formatVector(vec types.Vector) map[string]interface{} {
	results := make([]interface{}, 0, len(vec))
	for _, s := range vec {
		results = append(results, map[string]interface{}{
			"metric": s.Labels,
			"value":  [2]interface{}{float64(s.T) / 1000.0, formatFloat(s.F)},
		})
	}
	return map[string]interface{}{
		"resultType": "vector",
		"result":     results,
	}
}

func formatMatrix(matrix types.Matrix) map[string]interface{} {
	results := make([]interface{}, 0, len(matrix))
	for _, s := range matrix {
		values := make([][2]interface{}, 0, len(s.Samples))
		for _, p := range s.Samples {
			values = append(values, [2]interface{}{float64(p.Timestamp) / 1000.0, formatFloat(p.Value)})
		}
		results = append(results, map[string]interface{}{
			"metric": s.Labels,
			"values": values,
		})
	}
	return map[string]interface{}{
		"resultType": "matrix",
		"result":     results,
	}
}

// formatFloat renders a float as a string, matching Prometheus conventions.
func formatFloat(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// tryDownsampledQuery attempts to execute the query via downsampled tiers.
// Returns (result, true) on success, (nil, false) to fall back to raw eval.
func (h *Handler) tryDownsampledQuery(
	r *http.Request,
	plan *translator.SQLPlan,
	start, end time.Time,
	step time.Duration,
	t0 time.Time,
) (*types.QueryResult, bool) {
	selectedTier := h.Cfg.Downsampling.SelectTier(step)
	if selectedTier == nil {
		return nil, false // step too small for any tier
	}

	// Tier resolution must be <= range window for accurate results.
	// E.g. rate(counter[5m]) can't use 1h tier — bucket is too coarse.
	// avg_over_time(gauge[30m]) can't use 1h tier either.
	// Fall back to finer tier or raw.
	if plan.RangeMs > 0 && selectedTier.Resolution.Duration.Milliseconds() > plan.RangeMs {
		// Try finer tiers
		finerFound := false
		for i := range h.Cfg.Downsampling.Tiers {
			t := &h.Cfg.Downsampling.Tiers[i]
			if t.Resolution.Duration.Milliseconds() <= plan.RangeMs && step >= t.MinStep.Duration {
				selectedTier = t
				finerFound = true
				break
			}
		}
		if !finerFound {
			return nil, false // no suitable tier, use raw
		}
	}

	segments := h.Cfg.Downsampling.QuerySegments(start, end, selectedTier)
	if len(segments) == 1 && segments[0].IsRaw {
		return nil, false // all raw, use normal path
	}

	// Resolve fingerprints from label cache
	if h.Pool.LabelCache == nil || !h.Pool.LabelCache.IsLoaded() {
		return nil, false
	}

	var matchers []nativech.LabelMatcher
	for _, m := range plan.Matchers {
		matchers = append(matchers, nativech.LabelMatcher{
			Name: m.Name, Op: m.Op, Value: m.Val,
		})
	}
	fps, ok := h.Pool.LabelCache.GetFingerprints(plan.MetricName, matchers)
	if !ok || len(fps) == 0 {
		return nil, false
	}

	// Convert string fps to uint64
	fpUints := make([]uint64, 0, len(fps))
	for _, fp := range fps {
		if v, err := strconv.ParseUint(fp, 10, 64); err == nil {
			fpUints = append(fpUints, v)
		}
	}

	// Get metric type
	metricType := h.Pool.GetMetricType(r.Context(), plan.MetricName)

	// Build segmented query — try top-level function first,
	// then inner function if top-level is a math wrapper (abs, ceil, sort, etc.)
	queryFn := plan.FuncName
	var mathPostProcess string
	sql, err := nativech.BuildSegmentedQuery(
		queryFn, metricType, plan.MetricName,
		segments, fpUints, step,
	)
	if errors.Is(err, nativech.ErrRequiresRawSamples) && plan.Inner != nil && plan.Inner.FuncName != "" {
		// Top-level unsupported (math wrapper) — try inner function
		queryFn = plan.Inner.FuncName
		mathPostProcess = plan.FuncName
		sql, err = nativech.BuildSegmentedQuery(
			queryFn, metricType, plan.MetricName,
			segments, fpUints, step,
		)
	}
	if errors.Is(err, nativech.ErrRequiresRawSamples) {
		slog.Debug("downsampling: function requires raw samples", "fn", queryFn)
		return nil, false
	}
	if err != nil {
		slog.Warn("downsampling: build query failed", "error", err)
		return nil, false
	}

	// Execute via pool — different paths for counter vs gauge functions
	isCounterFn := queryFn == "rate" || queryFn == "increase"
	isGaugeFn := queryFn == "avg_over_time" || queryFn == "min_over_time" || queryFn == "max_over_time" || queryFn == "sum_over_time" || queryFn == "count_over_time"
	var matrix types.Matrix

	if isCounterFn {
		// Fast path: when range <= 2*step, each step needs ≤2 buckets.
		// Use ExecTierQueryRaw + lightweight sliding window (43ms for 1111 series).
		seriesMap, err := h.Pool.ExecTierQueryRaw(r.Context(), sql, fps)
		if err != nil {
			slog.Warn("downsampling: exec failed, falling back to raw", "error", err)
			return nil, false
		}
		matrix = windowedCounterEval(seriesMap, queryFn, plan.RangeMs, start, end, step)
	} else if isGaugeFn {
		gaugeMap, err := h.Pool.ExecGaugeQuery(r.Context(), sql, fps)
		if err != nil {
			slog.Warn("downsampling: exec failed, falling back to raw", "error", err)
			return nil, false
		}
		// SQL push-down: data is already GROUP BY step, just pick the right column
		matrix = gaugeToMatrix(gaugeMap, queryFn)
	} else {
		var err error
		matrix, err = h.Pool.ExecTierQuery(r.Context(), sql, fps)
		if err != nil {
			slog.Warn("downsampling: exec failed, falling back to raw", "error", err)
			return nil, false
		}
	}

	// Apply aggregation chain FIRST (e.g. sum by(region)(rate(...)))
	// Math post-processing (clamp, abs, sort) wraps the aggregate, so must come after.
	if len(plan.AggChain) > 0 {
		var aggErr error
		matrix, aggErr = applyAggChain(matrix, plan.AggChain, h.Pool.LabelCache)
		if errors.Is(aggErr, ErrUnsupportedAggOp) {
			slog.Debug("downsampling: unsupported agg op, falling back to raw", "chain", plan.AggChain)
			return nil, false
		}
	}

	// Apply math post-processing AFTER aggregation (abs, ceil, clamp, sort wrap the aggregate)
	if mathPostProcess != "" {
		matrix = applyMathFunc(matrix, mathPostProcess, plan)
	}

	series := len(matrix)
	slog.Info("query_range",
		"query", plan.MetricName,
		"path", "downsampled",
		"tier", selectedTier.Name,
		"segments", len(segments),
		"range", end.Sub(start).String(),
		"step", step.String(),
		"series", series,
		"total", time.Since(t0),
	)

	return &types.QueryResult{
		Type:   "matrix",
		Matrix: matrix,
	}, true
}

// windowedCounterEval evaluates rate/increase on per-bucket tier data
// with Prometheus-compatible extrapolation using first_time/last_time.
// Uses sliding window with two pointers — O(steps + buckets) per series
// instead of O(steps × buckets).
func windowedCounterEval(
	seriesMap map[string]*nativech.CounterSeries,
	fn string,
	rangeMs int64,
	start, end time.Time,
	step time.Duration,
) types.Matrix {
	var steps []int64
	if step <= 0 {
		steps = []int64{end.UnixMilli()}
	} else {
		n := int(end.Sub(start)/step) + 1
		steps = make([]int64, 0, n)
		for t := start; !t.After(end); t = t.Add(step) {
			steps = append(steps, t.UnixMilli())
		}
	}

	rangeSec := float64(rangeMs) / 1000.0

	var matrix types.Matrix
	for _, cs := range seriesMap {
		buckets := cs.Buckets // already sorted by timestamp from CH ORDER BY

		// Sliding window with two pointers — O(steps + buckets) per series
		samples := make([]types.Sample, 0, len(steps))
		lo := 0

		for _, evalTimeMs := range steps {
			windowStart := evalTimeMs - rangeMs
			windowEnd := evalTimeMs

			for lo < len(buckets) && buckets[lo].Timestamp <= windowStart {
				lo++
			}

			var sumDelta float64
			var firstTime, lastTime int64
			count := 0
			for i := lo; i < len(buckets) && buckets[i].Timestamp <= windowEnd; i++ {
				b := &buckets[i]
				sumDelta += b.CounterTotal
				if count == 0 {
					firstTime = b.FirstTime
					lastTime = b.LastTime
				} else {
					if b.FirstTime < firstTime {
						firstTime = b.FirstTime
					}
					if b.LastTime > lastTime {
						lastTime = b.LastTime
					}
				}
				count++
			}
			if count == 0 {
				continue
			}

			// Extrapolation: scale delta to cover full range window.
			// actualSpanMs = time span of actual samples in the window.
			// Cap at 1.05x to avoid over-extrapolation (Prometheus uses per-edge
			// logic which averages to ~1.0-1.03x for typical scrape intervals).
			actualSpanMs := float64(lastTime - firstTime)
			extrapolationFactor := 1.0
			if actualSpanMs > 0 && count > 1 {
				extrapolationFactor = float64(rangeMs) / actualSpanMs
				if extrapolationFactor > 1.05 {
					extrapolationFactor = 1.05
				}
			}

			var value float64
			switch fn {
			case "rate":
				value = sumDelta * extrapolationFactor / rangeSec
			case "increase":
				value = sumDelta * extrapolationFactor
			}
			samples = append(samples, types.Sample{Timestamp: evalTimeMs, Value: value})
		}
		if len(samples) > 0 {
			matrix = append(matrix, types.Series{Labels: cs.Labels, Samples: samples})
		}
	}
	return matrix
}

// gaugeToMatrix converts pre-aggregated gauge data (already GROUP BY step in SQL) to matrix.
// No windowed eval — each row is one step result.
func gaugeToMatrix(seriesMap map[string]*nativech.GaugeSeries, fn string) types.Matrix {
	matrix := make(types.Matrix, 0, len(seriesMap))
	for _, gs := range seriesMap {
		samples := make([]types.Sample, 0, len(gs.Buckets))
		for _, b := range gs.Buckets {
			var value float64
			switch fn {
			case "avg_over_time":
				if b.ValCount > 0 {
					value = b.ValSum / float64(b.ValCount)
				}
			case "min_over_time":
				value = b.ValMin
			case "max_over_time":
				value = b.ValMax
			case "sum_over_time":
				value = b.ValSum
			case "count_over_time":
				value = float64(b.ValCount)
			}
			samples = append(samples, types.Sample{Timestamp: b.Timestamp, Value: value})
		}
		if len(samples) > 0 {
			matrix = append(matrix, types.Series{Labels: gs.Labels, Samples: samples})
		}
	}
	return matrix
}

// windowedGaugeEval evaluates avg/min/max/sum/count_over_time on per-bucket tier data.
// Uses sliding window with two pointers — O(steps + buckets) per series.
func windowedGaugeEval(
	seriesMap map[string]*nativech.GaugeSeries,
	fn string,
	rangeMs int64,
	start, end time.Time,
	step time.Duration,
) types.Matrix {
	var steps []int64
	if step <= 0 {
		steps = []int64{end.UnixMilli()}
	} else {
		n := int(end.Sub(start)/step) + 1
		steps = make([]int64, 0, n)
		for t := start; !t.After(end); t = t.Add(step) {
			steps = append(steps, t.UnixMilli())
		}
	}

	var matrix types.Matrix
	for _, gs := range seriesMap {
		buckets := gs.Buckets // already sorted by timestamp from CH ORDER BY

		// Sliding window with two pointers — O(steps + buckets) per series
		samples := make([]types.Sample, 0, len(steps))
		lo := 0

		for _, evalTimeMs := range steps {
			windowStart := evalTimeMs - rangeMs
			windowEnd := evalTimeMs

			for lo < len(buckets) && buckets[lo].Timestamp <= windowStart {
				lo++
			}

			var sumVal, minVal, maxVal float64
			var countVal uint64
			first := true

			for i := lo; i < len(buckets) && buckets[i].Timestamp <= windowEnd; i++ {
				b := &buckets[i]
				sumVal += b.ValSum
				countVal += b.ValCount
				if first {
					minVal = b.ValMin
					maxVal = b.ValMax
					first = false
				} else {
					if b.ValMin < minVal {
						minVal = b.ValMin
					}
					if b.ValMax > maxVal {
						maxVal = b.ValMax
					}
				}
			}
			if first {
				continue
			}

			var value float64
			switch fn {
			case "avg_over_time":
				if countVal > 0 {
					value = sumVal / float64(countVal)
				}
			case "min_over_time":
				value = minVal
			case "max_over_time":
				value = maxVal
			case "sum_over_time":
				value = sumVal
			case "count_over_time":
				value = float64(countVal)
			}
			samples = append(samples, types.Sample{Timestamp: evalTimeMs, Value: value})
		}
		if len(samples) > 0 {
			matrix = append(matrix, types.Series{Labels: gs.Labels, Samples: samples})
		}
	}
	return matrix
}

// ErrUnsupportedAggOp signals that an aggregation operator is not supported on downsampled data.
var ErrUnsupportedAggOp = errors.New("unsupported aggregation operator on downsampled tier")

// applyAggChain applies aggregation steps to a matrix (e.g. sum by(region)).
func applyAggChain(matrix types.Matrix, chain []translator.AggStep, lc *nativech.LabelCache) (types.Matrix, error) {
	var err error
	for _, step := range chain {
		matrix, err = applyAggStep(matrix, step, lc)
		if err != nil {
			return nil, err
		}
	}
	return matrix, nil
}

func applyAggStep(matrix types.Matrix, step translator.AggStep, lc *nativech.LabelCache) (types.Matrix, error) {
	switch step.Op {
	case "sum", "avg", "min", "max", "count":
		return aggregateMatrix(matrix, step), nil
	case "topk":
		k := int(step.Param)
		if k > len(matrix) {
			k = len(matrix)
		}
		sort.Slice(matrix, func(i, j int) bool {
			return lastValue(matrix[i]) > lastValue(matrix[j])
		})
		return matrix[:k], nil
	case "bottomk":
		k := int(step.Param)
		if k > len(matrix) {
			k = len(matrix)
		}
		sort.Slice(matrix, func(i, j int) bool {
			return lastValue(matrix[i]) < lastValue(matrix[j])
		})
		return matrix[:k], nil
	case "group":
		result := aggregateMatrix(matrix, step)
		for i := range result {
			for j := range result[i].Samples {
				result[i].Samples[j].Value = 1.0
			}
		}
		return result, nil
	case "quantile":
		return aggregateMatrixQuantile(matrix, step), nil
	default:
		return nil, ErrUnsupportedAggOp
	}
}

// aggregateMatrixQuantile computes quantile(q) across series per group per timestamp.
func aggregateMatrixQuantile(matrix types.Matrix, step translator.AggStep) types.Matrix {
	q := step.Param

	type groupData struct {
		labels  map[string]string
		samples map[int64][]float64
	}
	groups := make(map[string]*groupData)

	for _, s := range matrix {
		key := groupKey(s.Labels, step.Grouping, step.Without)
		g, ok := groups[key]
		if !ok {
			g = &groupData{
				labels:  groupLabels(s.Labels, step.Grouping, step.Without),
				samples: make(map[int64][]float64),
			}
			groups[key] = g
		}
		for _, p := range s.Samples {
			g.samples[p.Timestamp] = append(g.samples[p.Timestamp], p.Value)
		}
	}

	var result types.Matrix
	for _, g := range groups {
		timestamps := make([]int64, 0, len(g.samples))
		for ts := range g.samples {
			timestamps = append(timestamps, ts)
		}
		sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

		samples := make([]types.Sample, 0, len(timestamps))
		for _, ts := range timestamps {
			vals := g.samples[ts]
			if len(vals) == 0 {
				continue
			}
			sort.Float64s(vals)
			idx := q * float64(len(vals)-1)
			lower := int(math.Floor(idx))
			upper := int(math.Ceil(idx))
			if lower == upper || upper >= len(vals) {
				samples = append(samples, types.Sample{Timestamp: ts, Value: vals[lower]})
			} else {
				frac := idx - float64(lower)
				v := vals[lower]*(1-frac) + vals[upper]*frac
				samples = append(samples, types.Sample{Timestamp: ts, Value: v})
			}
		}
		result = append(result, types.Series{Labels: g.labels, Samples: samples})
	}
	return result
}

func lastValue(s types.Series) float64 {
	if len(s.Samples) == 0 {
		return 0
	}
	return s.Samples[len(s.Samples)-1].Value
}

// aggregateMatrix groups series by labels and applies an aggregate function.
func aggregateMatrix(matrix types.Matrix, step translator.AggStep) types.Matrix {
	// Build group key per series
	type groupData struct {
		labels  map[string]string
		samples map[int64][]float64 // timestamp → values
	}
	groups := make(map[string]*groupData)

	for _, s := range matrix {
		key := groupKey(s.Labels, step.Grouping, step.Without)
		g, ok := groups[key]
		if !ok {
			g = &groupData{
				labels:  groupLabels(s.Labels, step.Grouping, step.Without),
				samples: make(map[int64][]float64),
			}
			groups[key] = g
		}
		for _, p := range s.Samples {
			g.samples[p.Timestamp] = append(g.samples[p.Timestamp], p.Value)
		}
	}

	// Aggregate each group
	var result types.Matrix
	for _, g := range groups {
		// Collect sorted timestamps
		tsMap := g.samples
		timestamps := make([]int64, 0, len(tsMap))
		for ts := range tsMap {
			timestamps = append(timestamps, ts)
		}
		sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

		samples := make([]types.Sample, 0, len(timestamps))
		for _, ts := range timestamps {
			vals := tsMap[ts]
			var v float64
			switch step.Op {
			case "sum":
				for _, x := range vals {
					v += x
				}
			case "avg":
				for _, x := range vals {
					v += x
				}
				v /= float64(len(vals))
			case "min":
				v = vals[0]
				for _, x := range vals[1:] {
					if x < v {
						v = x
					}
				}
			case "max":
				v = vals[0]
				for _, x := range vals[1:] {
					if x > v {
						v = x
					}
				}
			case "count":
				v = float64(len(vals))
			}
			samples = append(samples, types.Sample{Timestamp: ts, Value: v})
		}
		result = append(result, types.Series{Labels: g.labels, Samples: samples})
	}
	return result
}

// groupKey builds a string key for grouping by labels.
func groupKey(labels map[string]string, grouping []string, without bool) string {
	if without {
		var parts []string
		for k, v := range labels {
			skip := false
			for _, g := range grouping {
				if k == g {
					skip = true
					break
				}
			}
			if !skip {
				parts = append(parts, k+"="+v)
			}
		}
		sort.Strings(parts)
		return strings.Join(parts, ",")
	}
	parts := make([]string, 0, len(grouping))
	for _, g := range grouping {
		parts = append(parts, g+"="+labels[g])
	}
	return strings.Join(parts, ",")
}

// groupLabels builds the output label set for a group.
func groupLabels(labels map[string]string, grouping []string, without bool) map[string]string {
	result := make(map[string]string)
	if without {
		for k, v := range labels {
			skip := false
			for _, g := range grouping {
				if k == g {
					skip = true
					break
				}
			}
			if !skip {
				result[k] = v
			}
		}
	} else {
		for _, g := range grouping {
			if v, ok := labels[g]; ok {
				result[g] = v
			}
		}
	}
	return result
}

// tryDownsampledHistogram handles histogram_quantile(φ, sum by(le)(rate(bucket[5m])))
// on downsampled tiers. Executes inner rate() on tier, applies sum by(le),
// then computes histogram_quantile in Go.
func (h *Handler) tryDownsampledHistogram(
	r *http.Request,
	plan *translator.SQLPlan,
	start, end time.Time,
	step time.Duration,
	t0 time.Time,
) (*types.QueryResult, bool) {
	// Use preserved inner function name (rate, increase, etc.)
	innerFn := plan.InnerFuncName
	if innerFn == "" {
		// Fallback: infer from metric name
		if strings.HasSuffix(plan.MetricName, "_bucket") {
			innerFn = "rate"
		} else {
			return nil, false
		}
	}

	phi := plan.AggParam // quantile φ (0.0 - 1.0)

	selectedTier := h.Cfg.Downsampling.SelectTier(step)
	if selectedTier == nil {
		return nil, false
	}

	segments := h.Cfg.Downsampling.QuerySegments(start, end, selectedTier)
	if len(segments) == 1 && segments[0].IsRaw {
		return nil, false
	}

	if h.Pool.LabelCache == nil || !h.Pool.LabelCache.IsLoaded() {
		return nil, false
	}

	var matchers []nativech.LabelMatcher
	for _, m := range plan.Matchers {
		matchers = append(matchers, nativech.LabelMatcher{
			Name: m.Name, Op: m.Op, Value: m.Val,
		})
	}
	fps, ok := h.Pool.LabelCache.GetFingerprints(plan.MetricName, matchers)
	if !ok || len(fps) == 0 {
		return nil, false
	}

	fpUints := make([]uint64, 0, len(fps))
	for _, fp := range fps {
		if v, err := strconv.ParseUint(fp, 10, 64); err == nil {
			fpUints = append(fpUints, v)
		}
	}

	metricType := h.Pool.GetMetricType(r.Context(), plan.MetricName)

	// Execute inner function on tier — returns per-fingerprint values
	sql, err := nativech.BuildSegmentedQuery(
		innerFn, metricType, plan.MetricName,
		segments, fpUints, step,
	)
	if err != nil {
		slog.Debug("downsampling histogram: build query failed", "error", err)
		return nil, false
	}

	matrix, err := h.Pool.ExecTierQuery(r.Context(), sql, fps)
	if err != nil {
		slog.Warn("downsampling histogram: exec failed", "error", err)
		return nil, false
	}

	// Apply aggregation chain (e.g. sum by(le)) from AggChain
	if len(plan.AggChain) > 0 {
		var aggErr error
		matrix, aggErr = applyAggChain(matrix, plan.AggChain, h.Pool.LabelCache)
		if errors.Is(aggErr, ErrUnsupportedAggOp) {
			slog.Debug("downsampling histogram: unsupported agg op, falling back to raw")
			return nil, false
		}
	}

	// Validate that result has "le" labels — required for histogram_quantile
	hasLE := false
	for _, s := range matrix {
		if _, ok := s.Labels["le"]; ok {
			hasLE = true
			break
		}
	}
	if !hasLE && len(matrix) > 0 {
		slog.Debug("downsampling histogram: no le labels in result, falling back to raw")
		return nil, false
	}

	// Now matrix has series grouped by "le" label.
	// Compute histogram_quantile per step timestamp.
	result := computeHistogramQuantile(phi, matrix, step)

	slog.Info("query_range",
		"query", plan.MetricName,
		"path", "downsampled+histogram",
		"tier", selectedTier.Name,
		"segments", len(segments),
		"phi", phi,
		"series", len(result),
		"total", time.Since(t0),
	)

	return &types.QueryResult{
		Type:   "matrix",
		Matrix: result,
	}, true
}

// computeHistogramQuantile computes histogram_quantile(φ) across le-grouped series per step.
func computeHistogramQuantile(phi float64, matrix types.Matrix, step time.Duration) types.Matrix {
	// Collect all timestamps
	tsSet := make(map[int64]bool)
	for _, s := range matrix {
		for _, p := range s.Samples {
			tsSet[p.Timestamp] = true
		}
	}
	timestamps := make([]int64, 0, len(tsSet))
	for ts := range tsSet {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

	// Build per-timestamp le→value map
	type leValue struct {
		le  float64
		val float64
	}

	// Index: series le label → value per timestamp
	seriesByLE := make(map[string]map[int64]float64) // le_string → ts → value
	leValues := make(map[string]float64)              // le_string → le_float
	for _, s := range matrix {
		leStr, ok := s.Labels["le"]
		if !ok {
			continue
		}
		leFloat, err := strconv.ParseFloat(leStr, 64)
		if err != nil {
			continue
		}
		leValues[leStr] = leFloat
		m := make(map[int64]float64, len(s.Samples))
		for _, p := range s.Samples {
			m[p.Timestamp] = p.Value
		}
		seriesByLE[leStr] = m
	}

	// For each timestamp, build buckets and compute quantile
	totalLE := len(leValues)
	var samples []types.Sample
	for _, ts := range timestamps {
		var buckets []eval.Bucket
		for leStr, leFloat := range leValues {
			if vals, ok := seriesByLE[leStr]; ok {
				if v, ok := vals[ts]; ok {
					buckets = append(buckets, eval.Bucket{UpperBound: leFloat, Count: v})
				}
			}
		}
		// Skip timestamp if too few buckets (sparse data) or no +Inf bucket
		if len(buckets) < 2 || (totalLE > 0 && len(buckets) < totalLE/2) {
			continue
		}
		v, _, ok := eval.HistogramQuantile(phi, buckets)
		if ok && !math.IsNaN(v) {
			samples = append(samples, types.Sample{Timestamp: ts, Value: v})
		}
	}

	if len(samples) == 0 {
		return nil
	}

	// histogram_quantile returns a single series (all le labels dropped)
	return types.Matrix{{
		Labels:  map[string]string{},
		Samples: samples,
	}}
}

// applyMathFunc applies a per-value math transformation to all samples in the matrix.
func applyMathFunc(matrix types.Matrix, fn string, plan *translator.SQLPlan) types.Matrix {
	// Extract params for clamp functions
	clampMin := plan.AggParam  // clamp_min param, or clamp min
	clampMax := plan.SmoothingTF // clamp max (stored in SmoothingTF)
	if fn == "clamp_max" {
		clampMax = plan.AggParam
	}

	transform := func(v float64) float64 {
		switch fn {
		case "abs":
			return math.Abs(v)
		case "ceil":
			return math.Ceil(v)
		case "floor":
			return math.Floor(v)
		case "round":
			return math.Round(v)
		case "exp":
			return math.Exp(v)
		case "sqrt":
			return math.Sqrt(v)
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
			if v < clampMin {
				return clampMin
			}
			return v
		case "clamp_max":
			if v > clampMax {
				return clampMax
			}
			return v
		case "clamp":
			if v < clampMin {
				return clampMin
			}
			if v > clampMax {
				return clampMax
			}
			return v
		default:
			return v
		}
	}

	// sort/sort_desc — sort series by last value, don't modify values
	if fn == "sort" || fn == "sort_desc" {
		sort.Slice(matrix, func(i, j int) bool {
			vi := lastValue(matrix[i])
			vj := lastValue(matrix[j])
			if fn == "sort_desc" {
				return vi > vj
			}
			return vi < vj
		})
		return matrix
	}

	// Apply transform to all samples
	for i := range matrix {
		for j := range matrix[i].Samples {
			matrix[i].Samples[j].Value = transform(matrix[i].Samples[j].Value)
		}
	}
	return matrix
}

// tryCacheOnlyAgg handles count/group aggregations on plain vector selectors
// directly from label cache — zero CH fetch, zero samples transfer.
// Works for: count by(X)(metric), group by(X)(metric)
func (h *Handler) tryCacheOnlyAgg(
	plan *translator.SQLPlan,
	start, end time.Time,
	step time.Duration,
	t0 time.Time,
) (*types.QueryResult, bool) {
	// Must have label cache
	if h.Pool == nil || h.Pool.LabelCache == nil || !h.Pool.LabelCache.IsLoaded() {
		return nil, false
	}

	// Must be a simple aggregation on a plain vector selector (no range function)
	if plan.FuncName != "" || plan.ExprType == "binary" {
		return nil, false
	}

	// Get the aggregation op — single-level only
	var aggOp string
	var grouping []string
	var without bool
	if len(plan.AggChain) == 1 {
		aggOp = plan.AggChain[0].Op
		grouping = plan.AggChain[0].Grouping
		without = plan.AggChain[0].Without
	} else if plan.AggOp != "" && len(plan.AggChain) == 0 {
		aggOp = plan.AggOp
		grouping = plan.Grouping
		without = plan.Without
	} else {
		return nil, false
	}

	// Only count and group can be answered from labels alone
	if aggOp != "count" && aggOp != "group" {
		return nil, false
	}

	if plan.MetricName == "" {
		return nil, false
	}

	// Get matchers
	var matchers []nativech.LabelMatcher
	for _, m := range plan.Matchers {
		matchers = append(matchers, nativech.LabelMatcher{Name: m.Name, Op: m.Op, Value: m.Val})
	}

	fps, ok := h.Pool.LabelCache.GetFingerprints(plan.MetricName, matchers)
	if !ok {
		return nil, false
	}

	// Group fingerprints by grouping labels
	type group struct {
		labels map[string]string
		count  int
	}
	groups := make(map[string]*group)
	var order []string

	for _, fp := range fps {
		labels, hit := h.Pool.LabelCache.GetLabels(fp)
		if !hit {
			continue
		}
		gl := eval.GroupLabelsExported(labels, grouping, without)
		key := eval.MatchingKey(gl, false, nil) // ignoring nothing = use all labels as key
		g, exists := groups[key]
		if !exists {
			g = &group{labels: gl}
			groups[key] = g
			order = append(order, key)
		}
		g.count++
	}

	// Build result
	var steps []int64
	if step <= 0 {
		steps = []int64{end.UnixMilli()}
	} else {
		for t := start; !t.After(end); t = t.Add(step) {
			steps = append(steps, t.UnixMilli())
		}
	}

	if len(steps) == 1 {
		// Instant query
		var vec types.Vector
		for _, key := range order {
			g := groups[key]
			val := float64(g.count)
			if aggOp == "group" {
				val = 1.0
			}
			vec = append(vec, types.InstantSample{
				Labels:      g.labels,
				Fingerprint: fingerprint.Compute(g.labels),
				T:           steps[0],
				F:           val,
			})
		}
		slog.Info("query", "query", plan.MetricName, "path", "cache-only", "series", len(vec), "total", time.Since(t0))
		return &types.QueryResult{Type: "vector", Vector: vec}, true
	}

	// Range query — same value repeated for each step
	var matrix types.Matrix
	for _, key := range order {
		g := groups[key]
		val := float64(g.count)
		if aggOp == "group" {
			val = 1.0
		}
		samples := make([]types.Sample, len(steps))
		for i, ts := range steps {
			samples[i] = types.Sample{Timestamp: ts, Value: val}
		}
		matrix = append(matrix, types.Series{
			Labels:      g.labels,
			Fingerprint: fingerprint.Compute(g.labels),
			Samples:     samples,
		})
	}

	slog.Info("query_range", "query", plan.MetricName, "path", "cache-only", "series", len(matrix), "total", time.Since(t0))
	return &types.QueryResult{Type: "matrix", Matrix: matrix}, true
}

func resultSeriesCount(qr *types.QueryResult) int {
	switch qr.Type {
	case "vector":
		return len(qr.Vector)
	case "matrix":
		return len(qr.Matrix)
	default:
		return 0
	}
}
