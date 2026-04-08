package translator

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/hinskii/promclick/config"
)

// Transpiler translates PromQL AST → SQLPlan.
type Transpiler struct {
	cfg   *config.Config
	start time.Time
	end   time.Time
	step  time.Duration
}

func New(cfg *config.Config, start, end time.Time, step time.Duration) *Transpiler {
	return &Transpiler{cfg: cfg, start: start, end: end, step: step}
}

// TranspileQuery parses PromQL and returns an SQLPlan.
func (t *Transpiler) TranspileQuery(promql string) (*SQLPlan, error) {
	expr, err := parser.ParseExpr(promql)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	return t.transpile(expr)
}

// transpile recursively translates an AST node.
func (t *Transpiler) transpile(expr parser.Expr) (*SQLPlan, error) {
	switch n := expr.(type) {
	case *parser.VectorSelector:
		return t.transpileVectorSelector(n)
	case *parser.MatrixSelector:
		return t.transpileMatrixSelector(n)
	case *parser.Call:
		return t.transpileCall(n)
	case *parser.AggregateExpr:
		return t.transpileAggregateExpr(n)
	case *parser.BinaryExpr:
		return t.transpileBinaryExpr(n)
	case *parser.UnaryExpr:
		return t.transpileUnaryExpr(n)
	case *parser.ParenExpr:
		return t.transpile(n.Expr)
	case *parser.NumberLiteral:
		return &SQLPlan{IsScalar: true, ScalarVal: n.Val}, nil
	case *parser.SubqueryExpr:
		return nil, fmt.Errorf("unsupported: subquery [d:step]")
	default:
		return nil, fmt.Errorf("unsupported expr type: %T", expr)
	}
}

func (t *Transpiler) transpileUnaryExpr(u *parser.UnaryExpr) (*SQLPlan, error) {
	inner, err := t.transpile(u.Expr)
	if err != nil {
		return nil, err
	}
	if u.Op == parser.SUB {
		// Negate: wrap as binary 0 - inner
		return &SQLPlan{
			ExprType:       "binary",
			BinaryOp:       "-",
			LHS:            &SQLPlan{IsScalar: true, ScalarVal: 0},
			RHS:            inner,
			VectorMatching: &VectorMatching{Card: "one-to-one"},
			Cfg:            t.cfg,
		}, nil
	}
	return inner, nil
}
