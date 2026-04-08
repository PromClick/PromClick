package eval

import (
	"math"
	"sort"

	"github.com/hinskii/promclick/types"
)

func AvgOverTime(s []types.Sample) (float64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	var sum, c float64
	for _, p := range s {
		sum, c = kahanSumInc(p.Value, sum, c)
	}
	n := float64(len(s))
	return sum/n + c/n, true
}

func SumOverTime(s []types.Sample) (float64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	var sum, c float64
	for _, p := range s {
		sum, c = kahanSumInc(p.Value, sum, c)
	}
	return sum + c, true
}

func CountOverTime(s []types.Sample) (float64, bool) {
	return float64(len(s)), len(s) > 0
}

func MinOverTime(s []types.Sample) (float64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	m := s[0].Value
	for _, p := range s[1:] {
		if p.Value < m || math.IsNaN(m) {
			m = p.Value
		}
	}
	return m, true
}

func MaxOverTime(s []types.Sample) (float64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	m := s[0].Value
	for _, p := range s[1:] {
		if p.Value > m || math.IsNaN(m) {
			m = p.Value
		}
	}
	return m, true
}

func LastOverTime(s []types.Sample) (float64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	return s[len(s)-1].Value, true
}

func PresentOverTime(s []types.Sample) (float64, bool) {
	return 1.0, len(s) > 0
}

// StdvarOverTime — population variance (Welford online + Kahan).
func StdvarOverTime(s []types.Sample) (float64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	var count, mean, cMean, aux, cAux float64
	for _, p := range s {
		count++
		delta := p.Value - (mean + cMean)
		mean, cMean = kahanSumInc(delta/count, mean, cMean)
		aux, cAux = kahanSumInc(delta*(p.Value-(mean+cMean)), aux, cAux)
	}
	return (aux + cAux) / count, true
}

func StddevOverTime(s []types.Sample) (float64, bool) {
	v, ok := StdvarOverTime(s)
	return math.Sqrt(v), ok
}

// QuantileOverTime — R-7 interpolation (identical to Prometheus).
func QuantileOverTime(phi float64, s []types.Sample) (float64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	if math.IsNaN(phi) {
		return math.NaN(), true
	}
	if phi < 0 {
		return math.Inf(-1), true
	}
	if phi > 1 {
		return math.Inf(+1), true
	}

	vals := make([]float64, len(s))
	for i, p := range s {
		vals[i] = p.Value
	}
	sort.Float64s(vals)

	n := float64(len(vals))
	rank := phi * (n - 1)
	lo := math.Max(0, math.Floor(rank))
	hi := math.Min(n-1, lo+1)
	w := rank - math.Floor(rank)
	return vals[int(lo)]*(1-w) + vals[int(hi)]*w, true
}

// MadOverTime — median(|xi - median(x)|).
func MadOverTime(s []types.Sample) (float64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	vals := make([]float64, len(s))
	for i, p := range s {
		vals[i] = p.Value
	}
	sort.Float64s(vals)
	median := quantileFromSorted(0.5, vals)

	devs := make([]float64, len(vals))
	for i, v := range vals {
		devs[i] = math.Abs(v - median)
	}
	sort.Float64s(devs)
	return quantileFromSorted(0.5, devs), true
}

func quantileFromSorted(phi float64, vals []float64) float64 {
	n := float64(len(vals))
	rank := phi * (n - 1)
	lo := int(math.Max(0, math.Floor(rank)))
	hi := int(math.Min(n-1, float64(lo)+1))
	w := rank - math.Floor(rank)
	return vals[lo]*(1-w) + vals[hi]*w
}
