package eval

import (
	"fmt"
	"math"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/hinskii/promclick/types"
)

// Limitk returns the first k series (Prometheus is NOT random).
func Limitk(k int, v types.Vector) types.Vector {
	if k <= 0 {
		return types.Vector{}
	}
	if k >= len(v) {
		r := make(types.Vector, len(v))
		copy(r, v)
		return r
	}
	r := make(types.Vector, k)
	copy(r, v[:k])
	return r
}

// LimitRatio — deterministic hash-based sampling.
// r ∈ [-1, 1]. Complement: limit_ratio(0.3) ∪ limit_ratio(-0.3) = full set.
func LimitRatio(r float64, v types.Vector) (types.Vector, error) {
	if r < -1 || r > 1 {
		return nil, fmt.Errorf("limit_ratio: r must be in [-1,1], got %f", r)
	}
	if r == 0 {
		return types.Vector{}, nil
	}

	result := make(types.Vector, 0, len(v)/2+1)
	for _, s := range v {
		offset := seriesOffset(s.Labels)
		if addRatioSample(r, offset) {
			result = append(result, s)
		}
	}
	return result, nil
}

func seriesOffset(labels map[string]string) float64 {
	needed := 0
	for k, v := range labels {
		needed += len(k) + 1 + len(v) + 1
	}
	b := make([]byte, 0, needed)
	for _, k := range sortedKeysMap(labels) {
		b = append(b, k...)
		b = append(b, 0xff)
		b = append(b, labels[k]...)
		b = append(b, 0xff)
	}
	return float64(xxhash.Sum64(b)) / float64(math.MaxUint64)
}

func addRatioSample(r, offset float64) bool {
	if r >= 0 {
		return offset < r
	}
	return offset >= 1+r
}

func sortedKeysMap(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
