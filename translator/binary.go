package translator

import (
	"github.com/prometheus/prometheus/promql/parser"
)

func (t *Transpiler) transpileBinaryExpr(b *parser.BinaryExpr) (*SQLPlan, error) {
	lhs, err := t.transpile(b.LHS)
	if err != nil {
		return nil, err
	}
	rhs, err := t.transpile(b.RHS)
	if err != nil {
		return nil, err
	}

	vm := &VectorMatching{Card: "one-to-one"}
	if b.VectorMatching != nil {
		vm = &VectorMatching{
			Card:           cardString(b.VectorMatching),
			On:             b.VectorMatching.On,
			MatchingLabels: b.VectorMatching.MatchingLabels,
			Include:        b.VectorMatching.Include,
		}
	}

	plan := &SQLPlan{
		ExprType:       "binary",
		BinaryOp:       b.Op.String(),
		ReturnBool:     b.ReturnBool,
		LHS:            lhs,
		RHS:            rhs,
		VectorMatching: vm,
		DataStartMs:    t.start.UnixMilli(),
		DataEndMs:      t.end.UnixMilli(),
		Cfg:            t.cfg,
	}

	// Detect scalar sides so the evaluator can skip fetching them from CH
	if rhs.IsScalar {
		plan.IsScalarRHS = true
		plan.ScalarRHS = rhs.ScalarVal
		plan.RHS = nil
	}
	if lhs.IsScalar {
		plan.IsScalarLHS = true
		plan.ScalarLHS = lhs.ScalarVal
		plan.LHS = nil
	}

	return plan, nil
}

func cardString(vm *parser.VectorMatching) string {
	if vm == nil {
		return "one-to-one"
	}
	switch vm.Card {
	case parser.CardManyToOne:
		return "many-to-one"
	case parser.CardOneToMany:
		return "one-to-many"
	default:
		return "one-to-one"
	}
}
