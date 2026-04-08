package translator

import (
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
)

func (t *Transpiler) transpileAggregateExpr(a *parser.AggregateExpr) (*SQLPlan, error) {
	var paramVal float64

	opName := a.Op.String()

	switch opName {
	case "topk", "bottomk", "quantile", "limitk", "limit_ratio":
		if num, ok := a.Param.(*parser.NumberLiteral); ok {
			paramVal = num.Val
		}
	}

	plan, err := t.transpile(a.Expr)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", opName, err)
	}

	step := AggStep{
		Op:       opName,
		Param:    paramVal,
		Grouping: a.Grouping,
		Without:  a.Without,
	}

	if opName == "count_values" {
		if s, ok := a.Param.(*parser.StringLiteral); ok {
			step.Label = s.Val
		}
	}

	// Append to chain — innermost first, outermost last
	plan.AggChain = append(plan.AggChain, step)

	// Also set legacy single-level fields for backward compat
	// (last step wins — used by code that reads AggOp directly)
	plan.AggOp = opName
	plan.AggParam = paramVal
	plan.Grouping = a.Grouping
	plan.Without = a.Without
	plan.ExprType = "aggregate"
	if opName == "count_values" {
		plan.AggLabel = step.Label
	}

	return plan, nil
}
