package eval

import (
	"fmt"
	"math"

	"github.com/PromClick/PromClick/types"
)

// DoubleExponentialSmoothing — Holt-Winters without seasonality.
// Identical to Prometheus funcDoubleExponentialSmoothing.
// sf, tf ∈ (0, 1).
func DoubleExponentialSmoothing(samples []types.Sample, sf, tf float64) (float64, error) {
	if sf <= 0 || sf >= 1 {
		return 0, fmt.Errorf("sf must be in (0,1), got %f", sf)
	}
	if tf <= 0 || tf >= 1 {
		return 0, fmt.Errorf("tf must be in (0,1), got %f", tf)
	}
	if len(samples) < 2 {
		return 0, fmt.Errorf("need at least 2 samples")
	}

	s1 := samples[0].Value
	b := samples[1].Value - samples[0].Value
	var s0 float64

	for i := 1; i < len(samples); i++ {
		if i > 1 {
			b = tf*(s1-s0) + (1-tf)*b
		}
		s0, s1 = s1, sf*samples[i].Value+(1-sf)*(s1+b)
	}

	if math.IsNaN(s1) {
		return 0, fmt.Errorf("result is NaN")
	}
	return s1, nil
}
