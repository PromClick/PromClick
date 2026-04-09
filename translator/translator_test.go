package translator

import (
	"testing"
	"time"

	"github.com/PromClick/PromClick/config"
)

func testConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Schema.SamplesTable = "samples"
	cfg.Schema.TimeSeriesTable = "time_series"
	cfg.Schema.Columns.MetricName = "metric_name"
	cfg.Schema.Columns.Timestamp = "unix_milli"
	cfg.Schema.Columns.Value = "value"
	cfg.Schema.Columns.Fingerprint = "fingerprint"
	cfg.Schema.Columns.Labels = "labels"
	cfg.Schema.LabelsType = "json"
	cfg.Schema.JSONExtractFunc = "JSONExtractString"
	cfg.Schema.TimestampIsInt = true
	cfg.Prometheus.StalenessSeconds = 300
	cfg.Prometheus.DefaultStep = 60 * time.Second
	return cfg
}

func newTestTranspiler() *Transpiler {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	return New(testConfig(), now.Add(-time.Hour), now, time.Minute)
}

// --- TranspileQuery: basic expressions ---

func TestTranspile_VectorSelector(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`up{job="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.MetricName != "up" {
		t.Errorf("MetricName = %q, want %q", plan.MetricName, "up")
	}
	if plan.ExprType != "vector" {
		t.Errorf("ExprType = %q, want %q", plan.ExprType, "vector")
	}
	if len(plan.Matchers) != 1 || plan.Matchers[0].Name != "job" || plan.Matchers[0].Val != "api" {
		t.Errorf("Matchers = %+v, want [{job = api}]", plan.Matchers)
	}
}

func TestTranspile_RateFunction(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`rate(http_requests_total[5m])`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.FuncName != "rate" {
		t.Errorf("FuncName = %q, want %q", plan.FuncName, "rate")
	}
	if plan.RangeMs != 300000 {
		t.Errorf("RangeMs = %d, want %d", plan.RangeMs, 300000)
	}
	if plan.MetricName != "http_requests_total" {
		t.Errorf("MetricName = %q", plan.MetricName)
	}
}

func TestTranspile_IncreaseFunction(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`increase(counter[10m])`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.FuncName != "increase" {
		t.Errorf("FuncName = %q", plan.FuncName)
	}
	if plan.RangeMs != 600000 {
		t.Errorf("RangeMs = %d", plan.RangeMs)
	}
}

func TestTranspile_SumAggregation(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`sum by(job)(rate(requests[5m]))`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExprType != "aggregate" {
		t.Errorf("ExprType = %q", plan.ExprType)
	}
	if len(plan.AggChain) != 1 {
		t.Fatalf("AggChain len = %d, want 1", len(plan.AggChain))
	}
	if plan.AggChain[0].Op != "sum" {
		t.Errorf("AggChain[0].Op = %q", plan.AggChain[0].Op)
	}
	if len(plan.AggChain[0].Grouping) != 1 || plan.AggChain[0].Grouping[0] != "job" {
		t.Errorf("Grouping = %v", plan.AggChain[0].Grouping)
	}
}

func TestTranspile_NestedAggregation(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`topk(3, count by(mode)(sum by(cpu,mode)(node_cpu)))`)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.AggChain) != 3 {
		t.Fatalf("AggChain len = %d, want 3", len(plan.AggChain))
	}
	if plan.AggChain[0].Op != "sum" {
		t.Errorf("chain[0] = %q, want sum", plan.AggChain[0].Op)
	}
	if plan.AggChain[1].Op != "count" {
		t.Errorf("chain[1] = %q, want count", plan.AggChain[1].Op)
	}
	if plan.AggChain[2].Op != "topk" {
		t.Errorf("chain[2] = %q, want topk", plan.AggChain[2].Op)
	}
	if plan.AggChain[2].Param != 3 {
		t.Errorf("chain[2].Param = %f, want 3", plan.AggChain[2].Param)
	}
}

func TestTranspile_BinaryExpr_ScalarRHS(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`up > 0`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExprType != "binary" {
		t.Errorf("ExprType = %q", plan.ExprType)
	}
	if plan.BinaryOp != ">" {
		t.Errorf("BinaryOp = %q", plan.BinaryOp)
	}
	if !plan.IsScalarRHS || plan.ScalarRHS != 0 {
		t.Errorf("IsScalarRHS=%v ScalarRHS=%f", plan.IsScalarRHS, plan.ScalarRHS)
	}
}

func TestTranspile_BinaryExpr_ScalarLHS(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`100 - up`)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.IsScalarLHS || plan.ScalarLHS != 100 {
		t.Errorf("IsScalarLHS=%v ScalarLHS=%f", plan.IsScalarLHS, plan.ScalarLHS)
	}
}

func TestTranspile_BinaryExpr_VectorVector(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`foo / bar`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.BinaryOp != "/" {
		t.Errorf("BinaryOp = %q", plan.BinaryOp)
	}
	if plan.LHS == nil || plan.RHS == nil {
		t.Fatal("LHS or RHS is nil")
	}
	if plan.LHS.MetricName != "foo" || plan.RHS.MetricName != "bar" {
		t.Errorf("LHS=%q RHS=%q", plan.LHS.MetricName, plan.RHS.MetricName)
	}
}

func TestTranspile_BinaryExpr_VectorMatching(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`foo * on(instance) group_left bar`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.VectorMatching == nil {
		t.Fatal("VectorMatching is nil")
	}
	if plan.VectorMatching.Card != "many-to-one" {
		t.Errorf("Card = %q, want many-to-one", plan.VectorMatching.Card)
	}
	if !plan.VectorMatching.On {
		t.Error("On should be true")
	}
}

func TestTranspile_NumberLiteral(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`42`)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.IsScalar || plan.ScalarVal != 42 {
		t.Errorf("IsScalar=%v ScalarVal=%f", plan.IsScalar, plan.ScalarVal)
	}
}

func TestTranspile_MathChain(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`abs(rate(counter[5m]))`)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.MathChain) != 1 || plan.MathChain[0].Fn != "abs" {
		t.Errorf("MathChain = %+v", plan.MathChain)
	}
	if plan.FuncName != "rate" {
		t.Errorf("FuncName should be rate (inner), got %q", plan.FuncName)
	}
}

func TestTranspile_ClampMin(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`clamp_min(up, 0.5)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.MathChain) != 1 {
		t.Fatalf("MathChain len = %d", len(plan.MathChain))
	}
	if plan.MathChain[0].Fn != "clamp_min" || plan.MathChain[0].Param != 0.5 {
		t.Errorf("MathChain[0] = %+v", plan.MathChain[0])
	}
}

func TestTranspile_HistogramQuantile(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`histogram_quantile(0.99, rate(request_duration_bucket[5m]))`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExprType != "histogram_quantile" && plan.FuncName != "histogram_quantile" {
		t.Errorf("ExprType=%q FuncName=%q", plan.ExprType, plan.FuncName)
	}
	if plan.AggParam != 0.99 {
		t.Errorf("AggParam = %f, want 0.99", plan.AggParam)
	}
}

func TestTranspile_Absent(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`absent(up{job="api"})`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExprType != "absent" && plan.FuncName != "absent" {
		t.Errorf("ExprType=%q FuncName=%q", plan.ExprType, plan.FuncName)
	}
	found := false
	for _, m := range plan.AbsentMatchers {
		if m.Name == "job" && m.Val == "api" {
			found = true
		}
	}
	if !found {
		t.Errorf("AbsentMatchers missing job=api: %+v", plan.AbsentMatchers)
	}
}

func TestTranspile_LabelReplace(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`label_replace(up, "dst", "$1", "src", "(.*)")`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.FuncName != "label_replace" {
		t.Errorf("FuncName = %q", plan.FuncName)
	}
	if len(plan.LabelFuncArgs) < 4 {
		t.Fatalf("LabelFuncArgs len = %d, want >= 4", len(plan.LabelFuncArgs))
	}
}

func TestTranspile_Offset(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`up offset 5m`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.OffsetMs != 300000 {
		t.Errorf("OffsetMs = %d, want 300000", plan.OffsetMs)
	}
}

func TestTranspile_Time(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`time()`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExprType != "time" {
		t.Errorf("ExprType = %q", plan.ExprType)
	}
}

func TestTranspile_SubqueryError(t *testing.T) {
	tr := newTestTranspiler()
	_, err := tr.TranspileQuery(`rate(http_requests_total[5m])[1h:5m]`)
	if err == nil {
		t.Error("expected error for subquery")
	}
}

func TestTranspile_Render_ContainsMetricName(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`up{job="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	sql, params := plan.Render()
	if sql == "" {
		t.Error("Render returned empty SQL")
	}
	if params == nil {
		t.Error("Render returned nil params")
	}
	// SQL should reference metric name parameter
	vals := params.URLValues()
	if vals.Get("param_metricName") != "up" {
		t.Errorf("param_metricName = %q", vals.Get("param_metricName"))
	}
}

func TestTranspile_OverTimeFunctions(t *testing.T) {
	fns := []string{"avg_over_time", "min_over_time", "max_over_time", "sum_over_time", "count_over_time"}
	for _, fn := range fns {
		t.Run(fn, func(t *testing.T) {
			tr := newTestTranspiler()
			plan, err := tr.TranspileQuery(fn + `(metric[5m])`)
			if err != nil {
				t.Fatal(err)
			}
			if plan.FuncName != fn {
				t.Errorf("FuncName = %q", plan.FuncName)
			}
			if plan.RangeMs != 300000 {
				t.Errorf("RangeMs = %d", plan.RangeMs)
			}
		})
	}
}

func TestTranspile_Without(t *testing.T) {
	tr := newTestTranspiler()
	plan, err := tr.TranspileQuery(`sum without(instance)(up)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.AggChain) != 1 {
		t.Fatalf("AggChain len = %d", len(plan.AggChain))
	}
	if !plan.AggChain[0].Without {
		t.Error("Without should be true")
	}
}
