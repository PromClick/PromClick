package eval

import (
	"math"
	"sort"

	"github.com/hinskii/promclick/types"
)

// StaleNaN — Prometheus staleness marker (signaling NaN).
const StaleNaNBits uint64 = 0x7FF0000000000002

func IsStaleNaN(v float64) bool {
	return math.Float64bits(v) == StaleNaNBits
}

// InstantValue zwraca wartość serii dla danego eval_time z oknem staleness.
// Zwraca (value, true) lub (0, false) gdy brak próbki lub stale.
// samples MUSZĄ być posortowane po Timestamp ASC.
func InstantValue(samples []types.Sample, evalTimeMs, stalenessMs int64) (float64, bool) {
	if len(samples) == 0 {
		return 0, false
	}

	// Ostatni indeks z Timestamp <= evalTimeMs
	idx := sort.Search(len(samples), func(i int) bool {
		return samples[i].Timestamp > evalTimeMs
	}) - 1

	if idx < 0 {
		return 0, false
	}
	s := samples[idx]

	// Staleness window: próbka nie starsza niż stalenessMs
	if s.Timestamp < evalTimeMs-stalenessMs {
		return 0, false
	}

	// StaleNaN: seria jest "stale" — nie zwracamy wartości
	if IsStaleNaN(s.Value) {
		return 0, false
	}

	return s.Value, true
}

// WindowSamples zwraca próbki w oknie (rangeStart, rangeEnd].
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
