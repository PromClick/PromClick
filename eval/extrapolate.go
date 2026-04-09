package eval

import (
	"math"

	"github.com/PromClick/PromClick/types"
)

// ExtrapolatedRate implementuje rate/increase/delta z Prometheus.
//
// Dispatch:
//
//	rate()     → ExtrapolatedRate(samples, start, end, true,  true)
//	increase() → ExtrapolatedRate(samples, start, end, true,  false)
//	delta()    → ExtrapolatedRate(samples, start, end, false, false)
func ExtrapolatedRate(samples []types.Sample,
	rangeStartMs, rangeEndMs int64, isCounter, isRate bool) (float64, bool) {

	n := len(samples)
	if n < 2 {
		return 0, false
	}
	first, last := samples[0], samples[n-1]

	// Counter reset correction
	var counterCorrection float64
	var lastVal float64
	for _, s := range samples {
		if isCounter && s.Value < lastVal {
			counterCorrection += lastVal
		}
		lastVal = s.Value
	}

	resultValue := last.Value - first.Value + counterCorrection

	// Dystanse do granic (sekundy)
	durationToStart := float64(first.Timestamp-rangeStartMs) / 1000.0
	durationToEnd := float64(rangeEndMs-last.Timestamp) / 1000.0
	sampledInterval := float64(last.Timestamp-first.Timestamp) / 1000.0
	avgInterval := sampledInterval / float64(n-1)

	// Counter zero-point capping
	if isCounter && resultValue > 0 && first.Value >= 0 {
		durationToZero := sampledInterval * (first.Value / resultValue)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	// Ekstrapolacja z progiem 1.1×
	extrapolateToInterval := sampledInterval
	threshold := avgInterval * 1.1

	if durationToStart < threshold {
		extrapolateToInterval += durationToStart
	} else {
		extrapolateToInterval += avgInterval / 2
	}
	if durationToEnd < threshold {
		extrapolateToInterval += durationToEnd
	} else {
		extrapolateToInterval += avgInterval / 2
	}

	resultValue *= extrapolateToInterval / sampledInterval

	if isRate {
		resultValue /= float64(rangeEndMs-rangeStartMs) / 1000.0
	}
	if isCounter && resultValue < 0 {
		resultValue = 0
	}
	return resultValue, true
}

func IRate(samples []types.Sample) (float64, bool) {
	if len(samples) < 2 {
		return 0, false
	}
	last := samples[len(samples)-1]
	prev := samples[len(samples)-2]
	dt := float64(last.Timestamp-prev.Timestamp) / 1000.0
	if dt == 0 {
		return 0, false
	}
	if last.Value < prev.Value {
		return last.Value / dt, true
	}
	return (last.Value - prev.Value) / dt, true
}

func IDelta(samples []types.Sample) (float64, bool) {
	if len(samples) < 2 {
		return 0, false
	}
	return samples[len(samples)-1].Value - samples[len(samples)-2].Value, true
}

// kahanSumInc — Kahan-Neumaier compensated summation.
//
//go:noinline
func kahanSumInc(inc, sum, c float64) (newSum, newC float64) {
	t := sum + inc
	switch {
	case math.IsInf(t, 0):
		c = 0
	case math.Abs(sum) >= math.Abs(inc):
		c += (sum - t) + inc
	default:
		c += (inc - t) + sum
	}
	return t, c
}

// linearRegression — OLS z Kahan-Neumaier summation.
func linearRegression(samples []types.Sample, interceptTimeMs int64) (slope, intercept float64, ok bool) {
	if len(samples) < 2 {
		return 0, 0, false
	}

	var n, sumX, cX, sumY, cY, sumXY, cXY, sumX2, cX2 float64
	constY := true
	initY := samples[0].Value

	for i, s := range samples {
		if constY && i > 0 && s.Value != initY {
			constY = false
		}
		n++
		x := float64(s.Timestamp-interceptTimeMs) / 1e3
		sumX, cX = kahanSumInc(x, sumX, cX)
		sumY, cY = kahanSumInc(s.Value, sumY, cY)
		sumXY, cXY = kahanSumInc(x*s.Value, sumXY, cXY)
		sumX2, cX2 = kahanSumInc(x*x, sumX2, cX2)
	}

	if constY {
		if math.IsInf(initY, 0) {
			return math.NaN(), math.NaN(), true
		}
		return 0, initY, true
	}

	sumX += cX
	sumY += cY
	sumXY += cXY
	sumX2 += cX2

	covXY := sumXY - sumX*sumY/n
	varX := sumX2 - sumX*sumX/n
	slope = covXY / varX
	intercept = sumY/n - slope*sumX/n
	return slope, intercept, true
}

// Deriv — slope in units/second.
func Deriv(samples []types.Sample) (float64, bool) {
	if len(samples) < 2 {
		return 0, false
	}
	slope, _, ok := linearRegression(samples, samples[0].Timestamp)
	return slope, ok
}

// PredictLinear — value t seconds after evalTimeMs.
func PredictLinear(samples []types.Sample, evalTimeMs int64, t float64) (float64, bool) {
	if len(samples) < 2 {
		return 0, false
	}
	slope, intercept, ok := linearRegression(samples, evalTimeMs)
	if !ok {
		return 0, false
	}
	return intercept + slope*t, true
}

func Resets(samples []types.Sample) float64 {
	var n float64
	for i := 1; i < len(samples); i++ {
		if samples[i].Value < samples[i-1].Value {
			n++
		}
	}
	return n
}

func Changes(samples []types.Sample) float64 {
	var n float64
	for i := 1; i < len(samples); i++ {
		if samples[i].Value != samples[i-1].Value {
			n++
		}
	}
	return n
}
