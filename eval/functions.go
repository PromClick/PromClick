package eval

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/PromClick/PromClick/types"
)

// EvalAbsent — absent() / absent_over_time().
// Returns {labels_from_equality_matchers}=1 if vector is empty.
// Regex matchers are IGNORED in result labels.
func EvalAbsent(vec types.Vector, matchers []LabelMatcher) types.Vector {
	if len(vec) > 0 {
		return nil
	}
	ls := make(map[string]string)
	for _, m := range matchers {
		if m.Op == "=" && m.Name != "__name__" {
			ls[m.Name] = m.Val
		}
	}
	return types.Vector{{Labels: ls, F: 1.0}}
}

// LabelMatcher is a simplified matcher for absent() evaluation.
type LabelMatcher struct {
	Name string
	Op   string
	Val  string
}

// EvalLabelReplace — label_replace(v, dst, replacement, src, regex).
// Regex auto-anchored: "^(?:regex)$"
// No match → dst unchanged (not removed!).
func EvalLabelReplace(vec types.Vector, dst, replacement, src, regexStr string) (types.Vector, error) {
	re, err := regexp.Compile("^(?:" + regexStr + ")$")
	if err != nil {
		return nil, fmt.Errorf("label_replace regex %q: %w", regexStr, err)
	}

	out := make(types.Vector, len(vec))
	for i, s := range vec {
		newLabels := copyMap(s.Labels)
		srcVal := s.Labels[src]
		if idx := re.FindStringSubmatchIndex(srcVal); idx != nil {
			res := re.ExpandString(nil, replacement, srcVal, idx)
			if len(res) == 0 {
				delete(newLabels, dst)
			} else {
				newLabels[dst] = string(res)
			}
		}
		out[i] = types.InstantSample{Labels: newLabels, T: s.T, F: s.F}
	}
	return out, nil
}

// EvalLabelJoin — label_join(v, dst, sep, src1, src2, ...).
// Missing src → "" (empty string).
func EvalLabelJoin(vec types.Vector, dst, sep string, srcs []string) types.Vector {
	out := make(types.Vector, len(vec))
	for i, s := range vec {
		parts := make([]string, len(srcs))
		for j, src := range srcs {
			parts[j] = s.Labels[src]
		}
		newLabels := copyMap(s.Labels)
		joined := strings.Join(parts, sep)
		if joined == "" {
			delete(newLabels, dst)
		} else {
			newLabels[dst] = joined
		}
		out[i] = types.InstantSample{Labels: newLabels, T: s.T, F: s.F}
	}
	return out
}

func copyMap(m map[string]string) map[string]string {
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
