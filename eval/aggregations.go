package eval

import (
	"math"
	"sort"
	"strconv"

	"github.com/hinskii/promclick/fingerprint"
	"github.com/hinskii/promclick/types"
)

// groupKey builds the grouping key.
func groupKey(labels map[string]string, grouping []string, without bool) string {
	return MatchingKey(labels, !without, grouping)
}

// AggregateVector performs aggregation on an instant vector.
func AggregateVector(vec types.Vector, op string, grouping []string, without bool) types.Vector {
	type group struct {
		labels                        map[string]string
		count                         int
		sum, kahanC                   float64
		min, max                      float64
		welfordMean, welfordM2        float64
		welfordCount                  int
		values                        []float64
	}

	groups := make(map[string]*group, len(vec)/2+1)
	order := make([]string, 0, len(vec)/2+1)
	var sampleT int64 // preserve timestamp from input samples

	for _, s := range vec {
		if sampleT == 0 && s.T != 0 {
			sampleT = s.T
		}
		key := groupKey(s.Labels, grouping, without)
		g, ok := groups[key]
		if !ok {
			gl := groupLabels(s.Labels, grouping, without)
			g = &group{labels: gl, min: math.NaN(), max: math.NaN()}
			groups[key] = g
			order = append(order, key)
		}

		g.count++
		switch op {
		case "sum":
			g.sum, g.kahanC = kahanSumInc(s.F, g.sum, g.kahanC)
		case "avg":
			n := float64(g.count)
			g.welfordMean += s.F/n - g.welfordMean/n
		case "min":
			if math.IsNaN(g.min) || s.F < g.min {
				g.min = s.F
			}
		case "max":
			if math.IsNaN(g.max) || s.F > g.max {
				g.max = s.F
			}
		case "stddev", "stdvar":
			g.welfordCount++
			delta := s.F - g.welfordMean
			g.welfordMean += delta / float64(g.welfordCount)
			g.welfordM2 += delta * (s.F - g.welfordMean)
		case "quantile", "topk", "bottomk":
			g.values = append(g.values, s.F)
		}
	}

	var result types.Vector
	for _, key := range order {
		g := groups[key]
		var val float64
		switch op {
		case "sum":
			val = g.sum + g.kahanC
		case "avg":
			val = g.welfordMean
		case "min":
			val = g.min
		case "max":
			val = g.max
		case "count":
			val = float64(g.count)
		case "group":
			val = 1.0
		case "stdvar":
			val = g.welfordM2 / float64(g.welfordCount)
		case "stddev":
			val = math.Sqrt(g.welfordM2 / float64(g.welfordCount))
		}
		if op != "topk" && op != "bottomk" && op != "quantile" {
			result = append(result, types.InstantSample{
				Labels:      g.labels,
				Fingerprint: fingerprint.Compute(g.labels),
				F:           val,
				T:           sampleT,
			})
		}
	}
	// Pre-compute sort keys to avoid repeated labelsKeyStr in comparator
	sortKeys := make([]string, len(result))
	for i := range result {
		sortKeys[i] = labelsKeyStr(result[i].Labels)
	}
	sort.Slice(result, func(i, j int) bool {
		return sortKeys[i] < sortKeys[j]
	})
	return result
}

// AggregateTopK — topk/bottomk per group, preserves original labels.
func AggregateTopK(k int, vec types.Vector, grouping []string, without, bottom bool) types.Vector {
	type grp struct{ samples types.Vector }
	groups := make(map[string]*grp, len(vec)/2+1)
	order := make([]string, 0, len(vec)/2+1)

	for _, s := range vec {
		key := groupKey(s.Labels, grouping, without)
		g, ok := groups[key]
		if !ok {
			g = &grp{}
			groups[key] = g
			order = append(order, key)
		}
		g.samples = append(g.samples, s)
	}

	var result types.Vector
	for _, key := range order {
		g := groups[key]
		sort.Slice(g.samples, func(i, j int) bool {
			if bottom {
				return g.samples[i].F < g.samples[j].F
			}
			return g.samples[i].F > g.samples[j].F
		})
		n := k
		if n > len(g.samples) {
			n = len(g.samples)
		}
		result = append(result, g.samples[:n]...)
	}
	return result
}

// AggregateCountValues counts per unique value.
func AggregateCountValues(labelName string, vec types.Vector, grouping []string, without bool) types.Vector {
	type grp struct {
		labels map[string]string
		counts map[string]int
	}
	groups := make(map[string]*grp, len(vec)/2+1)
	order := make([]string, 0, len(vec)/2+1)

	for _, s := range vec {
		key := groupKey(s.Labels, grouping, without)
		g, ok := groups[key]
		if !ok {
			gl := groupLabels(s.Labels, grouping, without)
			g = &grp{labels: gl, counts: make(map[string]int)}
			groups[key] = g
			order = append(order, key)
		}
		sv := strconv.FormatFloat(s.F, 'f', -1, 64)
		g.counts[sv]++
	}

	var result types.Vector
	for _, key := range order {
		g := groups[key]
		for sv, cnt := range g.counts {
			out := copyMap(g.labels)
			out[labelName] = sv
			result = append(result, types.InstantSample{
				Labels:      out,
				Fingerprint: fingerprint.Compute(out),
				F:           float64(cnt),
			})
		}
	}
	return result
}

func aggregateQuantile(phi float64, vec types.Vector, grouping []string, without bool) types.Vector {
	type grp struct {
		labels map[string]string
		values []float64
	}
	groups := make(map[string]*grp, len(vec)/2+1)
	order := make([]string, 0, len(vec)/2+1)

	for _, s := range vec {
		key := groupKey(s.Labels, grouping, without)
		g, ok := groups[key]
		if !ok {
			gl := groupLabels(s.Labels, grouping, without)
			g = &grp{labels: gl}
			groups[key] = g
			order = append(order, key)
		}
		g.values = append(g.values, s.F)
	}

	var result types.Vector
	for _, key := range order {
		g := groups[key]
		sort.Float64s(g.values)
		val := quantileValue(phi, g.values)
		result = append(result, types.InstantSample{
			Labels:      g.labels,
			Fingerprint: fingerprint.Compute(g.labels),
			F:           val,
		})
	}
	return result
}

func quantileValue(phi float64, sorted []float64) float64 {
	if len(sorted) == 0 || math.IsNaN(phi) {
		return math.NaN()
	}
	if phi < 0 {
		return math.Inf(-1)
	}
	if phi > 1 {
		return math.Inf(1)
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := phi * float64(len(sorted)-1)
	lo := int(math.Floor(rank))
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	frac := rank - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

// GroupLabelsExported is the exported version of groupLabels for use by handlers.
func GroupLabelsExported(ls map[string]string, grouping []string, without bool) map[string]string {
	return groupLabels(ls, grouping, without)
}

func groupLabels(ls map[string]string, grouping []string, without bool) map[string]string {
	if without {
		out := make(map[string]string, len(ls))
		ign := make(map[string]struct{}, len(grouping))
		for _, k := range grouping {
			ign[k] = struct{}{}
		}
		for k, v := range ls {
			if _, skip := ign[k]; !skip {
				out[k] = v
			}
		}
		return out
	}
	out := make(map[string]string, len(grouping))
	for _, k := range grouping {
		if v, ok := ls[k]; ok {
			out[k] = v
		}
	}
	return out
}
