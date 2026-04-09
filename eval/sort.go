package eval

import (
	"slices"
	"strconv"
	"unicode"

	"github.com/PromClick/PromClick/types"
)

func SortByLabel(v types.Vector, labels ...string) types.Vector {
	result := make(types.Vector, len(v))
	copy(result, v)
	slices.SortStableFunc(result, func(a, b types.InstantSample) int {
		for _, lbl := range labels {
			la, lb := a.Labels[lbl], b.Labels[lbl]
			if la == lb {
				continue
			}
			if naturalLess(la, lb) {
				return -1
			}
			return +1
		}
		return 0
	})
	return result
}

func SortByLabelDesc(v types.Vector, labels ...string) types.Vector {
	result := SortByLabel(v, labels...)
	slices.Reverse(result)
	return result
}

func naturalLess(a, b string) bool {
	ia, ib := 0, 0
	for ia < len(a) && ib < len(b) {
		ca, cb := rune(a[ia]), rune(b[ib])
		aD, bD := unicode.IsDigit(ca), unicode.IsDigit(cb)
		switch {
		case aD && bD:
			na, ni := extractNum(a, ia)
			nb, ni2 := extractNum(b, ib)
			if na != nb {
				return na < nb
			}
			ia, ib = ni, ni2
		case ca == cb:
			ia++
			ib++
		default:
			return ca < cb
		}
	}
	return len(a) < len(b)
}

func extractNum(s string, start int) (int64, int) {
	end := start
	for end < len(s) && unicode.IsDigit(rune(s[end])) {
		end++
	}
	n, _ := strconv.ParseInt(s[start:end], 10, 64)
	return n, end
}
