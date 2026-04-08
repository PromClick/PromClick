package eval

import (
	"cmp"
	"math"
	"slices"
	"sort"
)

// Bucket — one bucket of a classic histogram.
type Bucket struct {
	UpperBound float64 // value of "le" label
	Count      float64 // cumulative observation count
}

const smallDeltaTolerance = 1e-12

// HistogramQuantile computes histogram_quantile(q, buckets).
// Returns (value, forcedMonotonic, ok).
func HistogramQuantile(q float64, buckets []Bucket) (float64, bool, bool) {
	switch {
	case math.IsNaN(q):
		return math.NaN(), false, true
	case q < 0:
		return math.Inf(-1), false, true
	case q > 1:
		return math.Inf(+1), false, true
	case len(buckets) == 0:
		return math.NaN(), false, false
	}

	slices.SortFunc(buckets, func(a, b Bucket) int {
		return cmp.Compare(a.UpperBound, b.UpperBound)
	})

	if !math.IsInf(buckets[len(buckets)-1].UpperBound, +1) {
		return math.NaN(), false, true
	}

	buckets = coalesceBuckets(buckets)
	forced := ensureMonotonic(buckets)

	if len(buckets) < 2 {
		return math.NaN(), false, true
	}

	observations := buckets[len(buckets)-1].Count
	if observations == 0 {
		return math.NaN(), false, true
	}

	rank := q * observations
	b := sort.Search(len(buckets)-1, func(i int) bool {
		return buckets[i].Count >= rank
	})

	var result float64
	switch {
	case b == len(buckets)-1:
		result = buckets[len(buckets)-2].UpperBound
	case b == 0 && buckets[0].UpperBound <= 0:
		result = buckets[0].UpperBound
	default:
		var bucketStart float64
		bucketEnd := buckets[b].UpperBound
		count := buckets[b].Count
		if b > 0 {
			bucketStart = buckets[b-1].UpperBound
			count -= buckets[b-1].Count
			rank -= buckets[b-1].Count
		}
		result = bucketStart + (bucketEnd-bucketStart)*(rank/count)
	}
	return result, forced, true
}

func coalesceBuckets(buckets []Bucket) []Bucket {
	if len(buckets) == 0 {
		return buckets
	}
	last := buckets[0]
	i := 0
	for _, b := range buckets[1:] {
		if b.UpperBound == last.UpperBound {
			last.Count += b.Count
		} else {
			buckets[i] = last
			last = b
			i++
		}
	}
	buckets[i] = last
	return buckets[:i+1]
}

func ensureMonotonic(buckets []Bucket) (forced bool) {
	prev := buckets[0].Count
	for i := 1; i < len(buckets); i++ {
		curr := buckets[i].Count
		if curr == prev {
			continue
		}
		if almostEqual(prev, curr, smallDeltaTolerance) {
			buckets[i].Count = prev
			continue
		}
		if curr < prev {
			buckets[i].Count = prev
			forced = true
			continue
		}
		prev = curr
	}
	return
}

func almostEqual(a, b, tol float64) bool {
	if a == b {
		return true
	}
	delta := math.Abs(a - b)
	if a == 0 || b == 0 {
		return delta < tol
	}
	return delta/math.Max(math.Abs(a), math.Abs(b)) < tol
}
