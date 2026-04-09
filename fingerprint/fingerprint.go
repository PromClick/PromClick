package fingerprint

import (
	"sort"

	"github.com/cespare/xxhash/v2"
)

// Compute calculates an xxhash64 fingerprint from labels.
// Sorts keys, builds "k\xffv\xff..." string, hashes.
// Compatible with qryn.
func Compute(labels map[string]string) uint64 {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	b := make([]byte, 0, 256)
	for _, k := range keys {
		b = append(b, k...)
		b = append(b, 0xff)
		b = append(b, labels[k]...)
		b = append(b, 0xff)
	}
	return xxhash.Sum64(b)
}
