package translator

import (
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
)

func (t *Transpiler) transpileCall(call *parser.Call) (*SQLPlan, error) {
	name := call.Func.Name

	// Range vector functions
	rangeVecFuncs := map[string]bool{
		"rate": true, "irate": true, "increase": true,
		"delta": true, "idelta": true,
		"deriv": true,
		"resets": true, "changes": true,
		"avg_over_time": true, "min_over_time": true, "max_over_time": true,
		"sum_over_time": true, "count_over_time": true,
		"stddev_over_time": true, "stdvar_over_time": true,
		"last_over_time": true, "present_over_time": true,
		"mad_over_time": true,
	}

	if rangeVecFuncs[name] {
		return t.transpileRangeFunc(call, name)
	}

	switch name {
	case "predict_linear":
		return t.transpilePredictLinear(call)
	case "quantile_over_time":
		return t.transpileQuantileOverTime(call)
	case "double_exponential_smoothing":
		return t.transpileDES(call)
	case "histogram_quantile":
		return t.transpileHistogramQuantile(call)
	case "absent":
		return t.transpileAbsent(call)
	case "absent_over_time":
		return t.transpileAbsentOverTime(call)
	case "label_replace":
		return t.transpileLabelReplace(call)
	case "label_join":
		return t.transpileLabelJoin(call)
	case "sort":
		return t.transpileSortFunc(call, false)
	case "sort_desc":
		return t.transpileSortFunc(call, true)
	case "sort_by_label":
		return t.transpileSortByLabel(call, false)
	case "sort_by_label_desc":
		return t.transpileSortByLabel(call, true)
	case "time":
		return &SQLPlan{ExprType: "time"}, nil
	case "vector":
		if num, ok := call.Args[0].(*parser.NumberLiteral); ok {
			return &SQLPlan{IsScalar: true, ScalarVal: num.Val}, nil
		}
		return nil, fmt.Errorf("vector(): argument must be number literal")
	case "scalar":
		inner, err := t.transpile(call.Args[0])
		if err != nil {
			return nil, err
		}
		inner.ExprType = "scalar_func"
		return inner, nil
	case "clamp":
		return t.transpileClamp(call)
	case "clamp_min":
		return t.transpileClampMinMax(call, "min")
	case "clamp_max":
		return t.transpileClampMinMax(call, "max")
	case "round":
		return t.transpileRound(call)
	}

	// Math / trig instant functions
	mathFuncs := map[string]bool{
		"abs": true, "ceil": true, "floor": true, "sqrt": true,
		"exp": true, "ln": true, "log2": true, "log10": true, "sgn": true,
		"sin": true, "cos": true, "tan": true,
		"asin": true, "acos": true, "atan": true,
		"sinh": true, "cosh": true, "tanh": true,
		"asinh": true, "acosh": true, "atanh": true,
		"deg": true, "rad": true, "pi": true,
	}
	if mathFuncs[name] {
		return t.transpileMathFunc(call, name)
	}

	return nil, fmt.Errorf("unsupported function: %s", name)
}

func (t *Transpiler) transpileRangeFunc(call *parser.Call, funcName string) (*SQLPlan, error) {
	ms, ok := call.Args[0].(*parser.MatrixSelector)
	if !ok {
		return nil, fmt.Errorf("%s: expected range vector, got %T", funcName, call.Args[0])
	}
	plan, err := t.transpileMatrixSelector(ms)
	if err != nil {
		return nil, err
	}
	plan.FuncName = funcName
	plan.ExprType = "call"
	return plan, nil
}

func (t *Transpiler) transpilePredictLinear(call *parser.Call) (*SQLPlan, error) {
	ms, ok := call.Args[0].(*parser.MatrixSelector)
	if !ok {
		return nil, fmt.Errorf("predict_linear: expected range vector")
	}
	tVal, ok2 := call.Args[1].(*parser.NumberLiteral)
	if !ok2 {
		return nil, fmt.Errorf("predict_linear: t must be number literal")
	}
	plan, err := t.transpileMatrixSelector(ms)
	if err != nil {
		return nil, err
	}
	plan.FuncName = "predict_linear"
	plan.ExprType = "call"
	plan.AggParam = tVal.Val
	return plan, nil
}

func (t *Transpiler) transpileQuantileOverTime(call *parser.Call) (*SQLPlan, error) {
	phi, ok := call.Args[0].(*parser.NumberLiteral)
	if !ok {
		return nil, fmt.Errorf("quantile_over_time: φ must be number")
	}
	ms, ok2 := call.Args[1].(*parser.MatrixSelector)
	if !ok2 {
		return nil, fmt.Errorf("quantile_over_time: expected range vector")
	}
	plan, err := t.transpileMatrixSelector(ms)
	if err != nil {
		return nil, err
	}
	plan.FuncName = "quantile_over_time"
	plan.ExprType = "call"
	plan.AggParam = phi.Val
	return plan, nil
}

func (t *Transpiler) transpileDES(call *parser.Call) (*SQLPlan, error) {
	ms, ok := call.Args[0].(*parser.MatrixSelector)
	if !ok {
		return nil, fmt.Errorf("double_exponential_smoothing: expected range vector")
	}
	sf, ok2 := call.Args[1].(*parser.NumberLiteral)
	tf, ok3 := call.Args[2].(*parser.NumberLiteral)
	if !ok2 || !ok3 {
		return nil, fmt.Errorf("double_exponential_smoothing: sf and tf must be numbers")
	}
	plan, err := t.transpileMatrixSelector(ms)
	if err != nil {
		return nil, err
	}
	plan.FuncName = "double_exponential_smoothing"
	plan.ExprType = "call"
	plan.AggParam = sf.Val
	plan.SmoothingTF = tf.Val
	return plan, nil
}

func (t *Transpiler) transpileHistogramQuantile(call *parser.Call) (*SQLPlan, error) {
	phi, ok := call.Args[0].(*parser.NumberLiteral)
	if !ok {
		return nil, fmt.Errorf("histogram_quantile: φ must be number")
	}
	inner, err := t.transpile(call.Args[1])
	if err != nil {
		return nil, err
	}
	inner.InnerFuncName = inner.FuncName // preserve "rate", "increase", etc.
	inner.FuncName = "histogram_quantile"
	inner.ExprType = "histogram_quantile"
	inner.AggParam = phi.Val
	return inner, nil
}

func (t *Transpiler) transpileAbsent(call *parser.Call) (*SQLPlan, error) {
	inner, err := t.transpile(call.Args[0])
	if err != nil {
		return nil, err
	}
	inner.FuncName = "absent"
	inner.ExprType = "absent"

	// Copy label matchers from VectorSelector for absent() synthesis
	if vs, ok := call.Args[0].(*parser.VectorSelector); ok {
		for _, m := range vs.LabelMatchers {
			inner.AbsentMatchers = append(inner.AbsentMatchers, LabelMatcher{
				Name: m.Name,
				Op:   m.Type.String(),
				Val:  m.Value,
			})
		}
	}
	return inner, nil
}

func (t *Transpiler) transpileAbsentOverTime(call *parser.Call) (*SQLPlan, error) {
	ms, ok := call.Args[0].(*parser.MatrixSelector)
	if !ok {
		return nil, fmt.Errorf("absent_over_time: expected range vector")
	}
	plan, err := t.transpileMatrixSelector(ms)
	if err != nil {
		return nil, err
	}
	plan.FuncName = "absent_over_time"
	plan.ExprType = "absent"
	return plan, nil
}

func (t *Transpiler) transpileLabelReplace(call *parser.Call) (*SQLPlan, error) {
	inner, err := t.transpile(call.Args[0])
	if err != nil {
		return nil, err
	}
	inner.FuncName = "label_replace"
	inner.ExprType = "label_func"
	inner.LabelFuncArgs = extractStringArgs(call.Args[1:])
	return inner, nil
}

func (t *Transpiler) transpileLabelJoin(call *parser.Call) (*SQLPlan, error) {
	inner, err := t.transpile(call.Args[0])
	if err != nil {
		return nil, err
	}
	inner.FuncName = "label_join"
	inner.ExprType = "label_func"
	inner.LabelFuncArgs = extractStringArgs(call.Args[1:])
	return inner, nil
}

func (t *Transpiler) transpileMathFunc(call *parser.Call, funcName string) (*SQLPlan, error) {
	if len(call.Args) == 0 && funcName == "pi" {
		return &SQLPlan{IsScalar: true, ScalarVal: 3.141592653589793}, nil
	}
	inner, err := t.transpile(call.Args[0])
	if err != nil {
		return nil, err
	}
	return wrapMathFunc(inner, funcName), nil
}

// wrapMathFunc adds a math function to the plan's MathChain.
// Math functions are applied AFTER aggregation in evalInstantSD.
// FuncName and Inner are NOT modified — the core function (rate, avg_over_time, etc.) stays.
func wrapMathFunc(inner *SQLPlan, funcName string) *SQLPlan {
	inner.MathChain = append(inner.MathChain, MathStep{Fn: funcName})
	return inner
}

func (t *Transpiler) transpileClamp(call *parser.Call) (*SQLPlan, error) {
	inner, err := t.transpile(call.Args[0])
	if err != nil {
		return nil, err
	}
	minV, _ := call.Args[1].(*parser.NumberLiteral)
	maxV, _ := call.Args[2].(*parser.NumberLiteral)
	inner.MathChain = append(inner.MathChain, MathStep{Fn: "clamp", Param: minV.Val, Param2: maxV.Val})
	return inner, nil
}

func (t *Transpiler) transpileClampMinMax(call *parser.Call, which string) (*SQLPlan, error) {
	inner, err := t.transpile(call.Args[0])
	if err != nil {
		return nil, err
	}
	val, _ := call.Args[1].(*parser.NumberLiteral)
	name := "clamp_min"
	if which != "min" {
		name = "clamp_max"
	}
	inner.MathChain = append(inner.MathChain, MathStep{Fn: name, Param: val.Val})
	return inner, nil
}

func (t *Transpiler) transpileRound(call *parser.Call) (*SQLPlan, error) {
	inner, err := t.transpile(call.Args[0])
	if err != nil {
		return nil, err
	}
	step := MathStep{Fn: "round", Param: 1}
	if len(call.Args) > 1 {
		if n, ok := call.Args[1].(*parser.NumberLiteral); ok {
			step.Param = n.Val
		}
	}
	inner.MathChain = append(inner.MathChain, step)
	return inner, nil
}

func (t *Transpiler) transpileSortFunc(call *parser.Call, desc bool) (*SQLPlan, error) {
	inner, err := t.transpile(call.Args[0])
	if err != nil {
		return nil, err
	}
	// Use wrapMathFunc to preserve Inner for downsampled path
	if desc {
		return wrapMathFunc(inner, "sort_desc"), nil
	}
	return wrapMathFunc(inner, "sort"), nil
}

func (t *Transpiler) transpileSortByLabel(call *parser.Call, desc bool) (*SQLPlan, error) {
	inner, err := t.transpile(call.Args[0])
	if err != nil {
		return nil, err
	}
	fn := "sort_by_label"
	if desc {
		fn = "sort_by_label_desc"
	}
	inner.MathChain = append(inner.MathChain, MathStep{
		Fn:         fn,
		SortLabels: extractStringArgs(call.Args[1:]),
	})
	return inner, nil
}

func extractStringArgs(args []parser.Expr) []string {
	result := make([]string, 0, len(args))
	for _, a := range args {
		if s, ok := a.(*parser.StringLiteral); ok {
			result = append(result, s.Val)
		}
	}
	return result
}
