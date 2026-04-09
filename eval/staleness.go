package eval

import (
	"math"
	"sort"

	"github.com/PromClick/PromClick/types"
)

// StaleNaN — Prometheus staleness marker (signaling NaN).
const StaleNaNBits uint64 = 0x7FF0000000000002

func IsStaleNaN(v float64) bool {
	return math.Float64bits(v) == StaleNaNBits
}

// InstantValue returns the series value for a given eval_time within the staleness window.
// Returns (value, true) or (0, false) when no sample is found or stale.
// samples MUST be sorted by Timestamp ASC.
func InstantValue(samples []types.Sample, evalTimeMs, stalenessMs int64) (float64, bool) {
	if len(samples) == 0 {
		return 0, false
	}

	// Last index with Timestamp <= evalTimeMs
	idx := sort.Search(len(samples), func(i int) bool {
		return samples[i].Timestamp > evalTimeMs
	}) - 1

	if idx < 0 {
		return 0, false
	}
	s := samples[idx]

	// Staleness window: sample not older than stalenessMs
	if s.Timestamp < evalTimeMs-stalenessMs {
		return 0, false
	}

	// StaleNaN: series is "stale" — do not return value
	if IsStaleNaN(s.Value) {
		return 0, false
	}

	return s.Value, true
}

// WindowSamples returns samples within the window (rangeStart, rangeEnd].
// Left-open, right-closed — identycznie jak Prometheus range selector.
func WindowSamples(samples []types.Sample, rangeStartMs, rangeEndMs int64) []types.Sample {
	lo := sort.Search(len(samples), func(i int) bool {
		return samples[i].Timestamp > rangeStartMs
	})
	hi := sort.Search(len(samples), func(i int) bool {
		return samples[i].Timestamp > rangeEndMs
	})
	if lo >= hi {
		return nil
	}
	return samples[lo:hi]
}
