package fingerprint

import (
	"sort"

	"github.com/cespare/xxhash/v2"
)

// Compute oblicza xxhash64 fingerprint z labeli.
// Sortuje klucze, buduje "k\xffv\xff..." string, hashuje.
// Kompatybilne z qryn.
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
