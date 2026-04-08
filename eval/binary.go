package eval

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/hinskii/promclick/fingerprint"
	"github.com/hinskii/promclick/types"
)

// ApplyBinaryOp applies a binary operator on two values.
// Returns (result, keep). keep=false for comparison filter mode.
func ApplyBinaryOp(lv, rv float64, op string) (float64, bool) {
	switch op {
	case "+":
		return lv + rv, true
	case "-":
		return lv - rv, true
	case "*":
		return lv * rv, true
	case "/":
		return lv / rv, true
	case "%":
		return math.Mod(lv, rv), true
	case "^":
		return math.Pow(lv, rv), true
	case "==":
		return lv, lv == rv
	case "!=":
		return lv, lv != rv
	case ">":
		return lv, lv > rv
	case "<":
		return lv, lv < rv
	case ">=":
		return lv, lv >= rv
	case "<=":
		return lv, lv <= rv
	case "atan2":
		return math.Atan2(lv, rv), true
	default:
		return 0, false
	}
}

// MatchingKey builds a matching key from labels.
// If on is true, only the given labels are used; otherwise all labels except those.
func MatchingKey(labels map[string]string, on bool, matching []string) string {
	set := make(map[string]struct{}, len(matching))
	for _, l := range matching {
		set[l] = struct{}{}
	}

	var keys []string
	if on {
		keys = append(keys, matching...)
	} else {
		for k := range labels {
			if _, skip := set[k]; !skip {
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(labels[k])
		b.WriteByte(1)
	}
	return b.String()
}

// VectorBinaryOp performs vector OP vector.
// Returns error for duplicate matches on the "one" side of many-to-one/one-to-many.
func VectorBinaryOp(lhs, rhs types.Vector, op string,
	matching VectorMatching, returnBool bool) (types.Vector, error) {

	// Build map for the "one" side; detect duplicate matches
	switch matching.Card {
	case "many-to-one":
		// RHS is "one" — must have unique keys
		rhsMap := make(map[string]types.InstantSample, len(rhs))
		for _, rs := range rhs {
			key := MatchingKey(rs.Labels, matching.On, matching.MatchingLabels)
			if _, exists := rhsMap[key]; exists {
				return nil, fmt.Errorf("multiple matches for labels: many-to-one matching must be unique on the right side")
			}
			rhsMap[key] = rs
		}
		return vectorBinOpWithMap(lhs, rhsMap, op, matching, returnBool, false), nil

	case "one-to-many":
		// LHS is "one" — must have unique keys
		lhsMap := make(map[string]types.InstantSample, len(lhs))
		for _, ls := range lhs {
			key := MatchingKey(ls.Labels, matching.On, matching.MatchingLabels)
			if _, exists := lhsMap[key]; exists {
				return nil, fmt.Errorf("multiple matches for labels: one-to-many matching must be unique on the left side")
			}
			lhsMap[key] = ls
		}
		// Swap: iterate RHS, lookup LHS
		return vectorBinOpWithMap(rhs, lhsMap, op, matching, returnBool, true), nil

	default: // one-to-one
		// Both sides must have unique keys
		rhsMap := make(map[string]types.InstantSample, len(rhs))
		for _, rs := range rhs {
			key := MatchingKey(rs.Labels, matching.On, matching.MatchingLabels)
			if _, exists := rhsMap[key]; exists {
				return nil, fmt.Errorf("multiple matches for labels: grouping labels must ensure unique matches")
			}
			rhsMap[key] = rs
		}
		// Also check LHS uniqueness
		lhsSeen := make(map[string]bool, len(lhs))
		for _, ls := range lhs {
			key := MatchingKey(ls.Labels, matching.On, matching.MatchingLabels)
			if lhsSeen[key] {
				return nil, fmt.Errorf("multiple matches for labels: grouping labels must ensure unique matches")
			}
			lhsSeen[key] = true
		}
		return vectorBinOpWithMap(lhs, rhsMap, op, matching, returnBool, false), nil
	}
}

// vectorBinOpWithMap applies the binary op by iterating iterSide and looking up in sideMap.
// If swapped is true, the map contains LHS samples and we iterate RHS.
func vectorBinOpWithMap(iterSide types.Vector, sideMap map[string]types.InstantSample,
	op string, matching VectorMatching, returnBool bool, swapped bool) types.Vector {

	var result types.Vector
	for _, s := range iterSide {
		key := MatchingKey(s.Labels, matching.On, matching.MatchingLabels)
		other, ok := sideMap[key]
		if !ok {
			continue
		}

		var ls, rs types.InstantSample
		if swapped {
			ls, rs = other, s // map=LHS, iter=RHS
		} else {
			ls, rs = s, other // iter=LHS, map=RHS
		}

		val, keep := ApplyBinaryOp(ls.F, rs.F, op)
		if !keep && !returnBool {
			continue
		}
		if returnBool {
			if keep {
				val = 1.0
			} else {
				val = 0.0
			}
		}

		outLabels := buildOutputLabels(ls, rs, matching)
		result = append(result, types.InstantSample{
			Labels:      outLabels,
			Fingerprint: fingerprint.Compute(outLabels),
			T:           ls.T,
			F:           val,
		})
	}
	return result
}

// VectorAnd implements the "and" set operator.
func VectorAnd(lhs, rhs types.Vector, m VectorMatching) types.Vector {
	rhsSet := make(map[string]struct{}, len(rhs))
	for _, rs := range rhs {
		rhsSet[MatchingKey(rs.Labels, m.On, m.MatchingLabels)] = struct{}{}
	}
	var out types.Vector
	for _, ls := range lhs {
		if _, ok := rhsSet[MatchingKey(ls.Labels, m.On, m.MatchingLabels)]; ok {
			out = append(out, ls)
		}
	}
	return out
}

// VectorOr implements the "or" set operator.
func VectorOr(lhs, rhs types.Vector, m VectorMatching) types.Vector {
	out := make(types.Vector, 0, len(lhs)+len(rhs))
	lhsSet := make(map[string]struct{}, len(lhs))
	for _, ls := range lhs {
		lhsSet[MatchingKey(ls.Labels, m.On, m.MatchingLabels)] = struct{}{}
		out = append(out, ls)
	}
	for _, rs := range rhs {
		if _, ok := lhsSet[MatchingKey(rs.Labels, m.On, m.MatchingLabels)]; !ok {
			out = append(out, rs)
		}
	}
	return out
}

// VectorUnless implements the "unless" set operator.
func VectorUnless(lhs, rhs types.Vector, m VectorMatching) types.Vector {
	rhsSet := make(map[string]struct{}, len(rhs))
	for _, rs := range rhs {
		rhsSet[MatchingKey(rs.Labels, m.On, m.MatchingLabels)] = struct{}{}
	}
	var out types.Vector
	for _, ls := range lhs {
		if _, ok := rhsSet[MatchingKey(ls.Labels, m.On, m.MatchingLabels)]; !ok {
			out = append(out, ls)
		}
	}
	return out
}

// VectorMatching describes how two vectors should be matched in binary ops.
type VectorMatching struct {
	Card           string   // "one-to-one" | "many-to-one" | "one-to-many"
	MatchingLabels []string // on(...) / ignoring(...)
	On             bool     // true=on, false=ignoring
	Include        []string // extra labels for group_left/group_right
}

func buildOutputLabels(ls, rs types.InstantSample, m VectorMatching) map[string]string {
	out := make(map[string]string)
	var base, other map[string]string
	if m.Card == "one-to-many" {
		base, other = rs.Labels, ls.Labels
	} else {
		base, other = ls.Labels, rs.Labels
	}

	if m.On {
		for _, k := range m.MatchingLabels {
			if v, ok := base[k]; ok {
				out[k] = v
			}
		}
	} else {
		ignSet := make(map[string]struct{})
		for _, k := range m.MatchingLabels {
			ignSet[k] = struct{}{}
		}
		for k, v := range base {
			if _, skip := ignSet[k]; !skip {
				out[k] = v
			}
		}
	}
	for _, k := range m.Include {
		if v, ok := other[k]; ok {
			out[k] = v
		}
	}
	return out
}
