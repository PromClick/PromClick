package eval

import (
	"math"
	"testing"

	"github.com/hinskii/promclick/types"
)

// === ExtrapolatedRate tests ===

func TestExtrapolatedRate_Basic(t *testing.T) {
	// Counter increasing linearly: 0, 10, 20, 30 over 45s window
	samples := []types.Sample{
		{Timestamp: 1000, Value: 0},
		{Timestamp: 16000, Value: 10},
		{Timestamp: 31000, Value: 20},
		{Timestamp: 46000, Value: 30},
	}
	// rate over [0, 60000] → 60s window
	val, ok := ExtrapolatedRate(samples, 0, 60000, true, true)
	if !ok {
		t.Fatal("expected ok")
	}
	// 30 increase over 45s, extrapolated to 60s = 40, rate = 40/60 ≈ 0.667
	if val < 0.5 || val > 0.8 {
		t.Errorf("rate = %f, expected ~0.667", val)
	}
}

func TestExtrapolatedRate_CounterReset(t *testing.T) {
	// Counter resets from 100 to 10
	samples := []types.Sample{
		{Timestamp: 0, Value: 90},
		{Timestamp: 15000, Value: 100},
		{Timestamp: 30000, Value: 10}, // reset
		{Timestamp: 45000, Value: 20},
	}
	val, ok := ExtrapolatedRate(samples, 0, 60000, true, true)
	if !ok {
		t.Fatal("expected ok")
	}
	// Counter correction: 100 (from reset) + (20-90) + 100 = 30 effective increase
	// counterCorrection accumulates lastVal when current < lastVal
	if val < 0 {
		t.Errorf("rate after reset should be positive, got %f", val)
	}
}

func TestExtrapolatedRate_Increase(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 0, Value: 100},
		{Timestamp: 60000, Value: 200},
	}
	// increase = rate * range = extrapolatedRate(isCounter=true, isRate=false)
	val, ok := ExtrapolatedRate(samples, 0, 60000, true, false)
	if !ok {
		t.Fatal("expected ok")
	}
	if val < 90 || val > 110 {
		t.Errorf("increase = %f, expected ~100", val)
	}
}

func TestExtrapolatedRate_Delta(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 0, Value: 100},
		{Timestamp: 60000, Value: 50},
	}
	// delta = extrapolatedRate(isCounter=false, isRate=false)
	// Can be negative for gauges
	val, ok := ExtrapolatedRate(samples, 0, 60000, false, false)
	if !ok {
		t.Fatal("expected ok")
	}
	if val > 0 {
		t.Errorf("delta should be negative for decreasing gauge, got %f", val)
	}
}

func TestExtrapolatedRate_TooFewSamples(t *testing.T) {
	samples := []types.Sample{{Timestamp: 0, Value: 100}}
	_, ok := ExtrapolatedRate(samples, 0, 60000, true, true)
	if ok {
		t.Fatal("expected not ok for < 2 samples")
	}
}

// === IRate tests ===

func TestIRate_Normal(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 0, Value: 100},
		{Timestamp: 15000, Value: 115},
	}
	val, ok := IRate(samples)
	if !ok {
		t.Fatal("expected ok")
	}
	// (115-100) / 15 = 1.0
	if math.Abs(val-1.0) > 0.001 {
		t.Errorf("irate = %f, expected 1.0", val)
	}
}

func TestIRate_CounterReset(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 0, Value: 100},
		{Timestamp: 15000, Value: 10}, // reset
	}
	val, ok := IRate(samples)
	if !ok {
		t.Fatal("expected ok")
	}
	// Counter reset: last.Value < prev.Value → return last.Value / dt = 10/15
	expected := 10.0 / 15.0
	if math.Abs(val-expected) > 0.001 {
		t.Errorf("irate after reset = %f, expected %f", val, expected)
	}
}

// === Staleness tests ===

func TestInstantValue_Basic(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 2000, Value: 2.0},
		{Timestamp: 3000, Value: 3.0},
	}
	val, ok := InstantValue(samples, 3000, 5000)
	if !ok || val != 3.0 {
		t.Errorf("expected 3.0, got %f ok=%v", val, ok)
	}
}

func TestInstantValue_Staleness(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 1000, Value: 1.0},
	}
	// evalTime=10000, staleness=5000 → sample at 1000 is 9000ms old > 5000
	_, ok := InstantValue(samples, 10000, 5000)
	if ok {
		t.Error("expected stale (not ok)")
	}
}

func TestInstantValue_StaleNaN(t *testing.T) {
	staleVal := math.Float64frombits(StaleNaNBits)
	samples := []types.Sample{
		{Timestamp: 1000, Value: staleVal},
	}
	_, ok := InstantValue(samples, 1000, 5000)
	if ok {
		t.Error("expected StaleNaN detection (not ok)")
	}
}

func TestWindowSamples(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 1000, Value: 1},
		{Timestamp: 2000, Value: 2},
		{Timestamp: 3000, Value: 3},
		{Timestamp: 4000, Value: 4},
	}
	// Window (1500, 3500] → samples at 2000, 3000
	w := WindowSamples(samples, 1500, 3500)
	if len(w) != 2 {
		t.Errorf("expected 2 samples, got %d", len(w))
	}
}

// === Over-time tests ===

func TestAvgOverTime(t *testing.T) {
	samples := []types.Sample{
		{Value: 1}, {Value: 2}, {Value: 3},
	}
	val, ok := AvgOverTime(samples)
	if !ok || math.Abs(val-2.0) > 0.001 {
		t.Errorf("avg = %f, expected 2.0", val)
	}
}

func TestStddevOverTime_Populational(t *testing.T) {
	// Values: 2, 4, 4, 4, 5, 5, 7, 9 → mean=5, population variance=4, stddev=2
	samples := []types.Sample{
		{Value: 2}, {Value: 4}, {Value: 4}, {Value: 4},
		{Value: 5}, {Value: 5}, {Value: 7}, {Value: 9},
	}
	val, ok := StddevOverTime(samples)
	if !ok {
		t.Fatal("expected ok")
	}
	if math.Abs(val-2.0) > 0.01 {
		t.Errorf("stddev = %f, expected 2.0 (population)", val)
	}
}

func TestQuantileOverTime_R7(t *testing.T) {
	samples := []types.Sample{
		{Value: 1}, {Value: 2}, {Value: 3}, {Value: 4}, {Value: 5},
	}
	// p50 of [1,2,3,4,5] with R-7: 3.0
	val, ok := QuantileOverTime(0.5, samples)
	if !ok || math.Abs(val-3.0) > 0.001 {
		t.Errorf("p50 = %f, expected 3.0", val)
	}
	// p0 = 1.0
	val, _ = QuantileOverTime(0.0, samples)
	if math.Abs(val-1.0) > 0.001 {
		t.Errorf("p0 = %f, expected 1.0", val)
	}
	// p100 = 5.0
	val, _ = QuantileOverTime(1.0, samples)
	if math.Abs(val-5.0) > 0.001 {
		t.Errorf("p100 = %f, expected 5.0", val)
	}
}

func TestMadOverTime(t *testing.T) {
	// Values: 1, 1, 2, 2, 4, 6, 9 → median=2, devs=|1-2|,|1-2|,|0|,|0|,|2|,|4|,|7| = 1,1,0,0,2,4,7 → sorted 0,0,1,1,2,4,7 → median=1
	samples := []types.Sample{
		{Value: 1}, {Value: 1}, {Value: 2}, {Value: 2},
		{Value: 4}, {Value: 6}, {Value: 9},
	}
	val, ok := MadOverTime(samples)
	if !ok {
		t.Fatal("expected ok")
	}
	if math.Abs(val-1.0) > 0.01 {
		t.Errorf("mad = %f, expected 1.0", val)
	}
}

// === Histogram quantile tests ===

func TestHistogramQuantile_Basic(t *testing.T) {
	buckets := []Bucket{
		{UpperBound: 0.1, Count: 10},
		{UpperBound: 0.5, Count: 50},
		{UpperBound: 1.0, Count: 90},
		{UpperBound: math.Inf(1), Count: 100},
	}
	val, _, ok := HistogramQuantile(0.5, buckets)
	if !ok {
		t.Fatal("expected ok")
	}
	// p50: rank=50, bucket [0.1, 0.5] has count 40 (50-10), rank in bucket = 50-10=40
	// result = 0.1 + (0.5-0.1)*(40/40) = 0.5
	if val < 0.4 || val > 0.6 {
		t.Errorf("p50 = %f, expected ~0.5", val)
	}
}

func TestHistogramQuantile_NoInfBucket(t *testing.T) {
	buckets := []Bucket{
		{UpperBound: 1.0, Count: 100},
	}
	val, _, ok := HistogramQuantile(0.5, buckets)
	if !ok {
		t.Fatal("expected ok")
	}
	if !math.IsNaN(val) {
		t.Errorf("expected NaN without +Inf bucket, got %f", val)
	}
}

func TestHistogramQuantile_Monotonicity(t *testing.T) {
	// Non-monotonic: bucket at 0.5 has higher count than bucket at 1.0
	buckets := []Bucket{
		{UpperBound: 0.5, Count: 100},
		{UpperBound: 1.0, Count: 50}, // non-monotonic
		{UpperBound: math.Inf(1), Count: 100},
	}
	_, forced, ok := HistogramQuantile(0.5, buckets)
	if !ok {
		t.Fatal("expected ok")
	}
	if !forced {
		t.Error("expected forced monotonicity")
	}
}

// === Binary ops tests ===

func TestApplyBinaryOp_Arithmetic(t *testing.T) {
	tests := []struct {
		op       string
		lv, rv   float64
		expected float64
	}{
		{"+", 1, 2, 3},
		{"-", 5, 3, 2},
		{"*", 3, 4, 12},
		{"/", 10, 4, 2.5},
		{"%", 10, 3, 1},
		{"^", 2, 3, 8},
	}
	for _, tt := range tests {
		val, keep := ApplyBinaryOp(tt.lv, tt.rv, tt.op)
		if !keep {
			t.Errorf("%s: expected keep=true", tt.op)
		}
		if math.Abs(val-tt.expected) > 0.001 {
			t.Errorf("%s: %f %s %f = %f, expected %f", tt.op, tt.lv, tt.op, tt.rv, val, tt.expected)
		}
	}
}

func TestApplyBinaryOp_Comparison(t *testing.T) {
	// > filter mode (no bool)
	val, keep := ApplyBinaryOp(5, 3, ">")
	if !keep || val != 5 {
		t.Errorf("> filter: val=%f keep=%v, expected val=5 keep=true", val, keep)
	}
	_, keep = ApplyBinaryOp(1, 3, ">")
	if keep {
		t.Error("> filter: expected keep=false for 1>3")
	}
}

func TestVectorBinaryOp_DuplicateDetection(t *testing.T) {
	lhs := types.Vector{
		{Labels: map[string]string{"a": "1"}, F: 10},
		{Labels: map[string]string{"a": "1"}, F: 20}, // duplicate key
	}
	rhs := types.Vector{
		{Labels: map[string]string{"a": "1"}, F: 5},
	}
	vm := VectorMatching{Card: "one-to-one", On: true, MatchingLabels: []string{"a"}}
	_, err := VectorBinaryOp(lhs, rhs, "+", vm, false)
	if err == nil {
		t.Error("expected error for duplicate LHS matches")
	}
}

// === Aggregation tests ===

func TestAggregateVector_SumBy(t *testing.T) {
	vec := types.Vector{
		{Labels: map[string]string{"job": "api", "instance": "a"}, F: 10},
		{Labels: map[string]string{"job": "api", "instance": "b"}, F: 20},
		{Labels: map[string]string{"job": "web", "instance": "c"}, F: 30},
	}
	result := AggregateVector(vec, "sum", []string{"job"}, false)
	if len(result) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result))
	}
	// Check that result labels only have "job" key
	for _, s := range result {
		if _, ok := s.Labels["instance"]; ok {
			t.Error("sum by(job) should not have 'instance' label")
		}
		if _, ok := s.Labels["job"]; !ok {
			t.Error("sum by(job) must have 'job' label")
		}
	}
}

func TestAggregateTopK_PreservesLabels(t *testing.T) {
	vec := types.Vector{
		{Labels: map[string]string{"job": "api", "host": "a"}, F: 100},
		{Labels: map[string]string{"job": "api", "host": "b"}, F: 200},
		{Labels: map[string]string{"job": "api", "host": "c"}, F: 50},
	}
	result := AggregateTopK(2, vec, []string{"job"}, false, false)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	// TopK should preserve original labels (including "host")
	for _, s := range result {
		if _, ok := s.Labels["host"]; !ok {
			t.Error("topk should preserve original labels including 'host'")
		}
	}
	// Check ordering: highest first
	if result[0].F < result[1].F {
		t.Error("topk should return highest first")
	}
}

// === Deriv / PredictLinear tests ===

func TestDeriv_LinearIncrease(t *testing.T) {
	// Perfect linear: 0, 1, 2, 3 over 3 seconds
	samples := []types.Sample{
		{Timestamp: 0, Value: 0},
		{Timestamp: 1000, Value: 1},
		{Timestamp: 2000, Value: 2},
		{Timestamp: 3000, Value: 3},
	}
	val, ok := Deriv(samples)
	if !ok {
		t.Fatal("expected ok")
	}
	// Slope should be 1.0 per second
	if math.Abs(val-1.0) > 0.001 {
		t.Errorf("deriv = %f, expected 1.0", val)
	}
}

func TestPredictLinear(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 0, Value: 0},
		{Timestamp: 1000, Value: 1},
		{Timestamp: 2000, Value: 2},
	}
	// Predict 10 seconds ahead from evalTime=2000
	val, ok := PredictLinear(samples, 2000, 10)
	if !ok {
		t.Fatal("expected ok")
	}
	// At t=2s: intercept ≈ 2, slope ≈ 1/s → predict at t=12s: ≈ 12
	if math.Abs(val-12.0) > 0.1 {
		t.Errorf("predict = %f, expected ~12.0", val)
	}
}

// === Scalar tests ===

func TestScalarBinaryOpVector(t *testing.T) {
	vec := types.Vector{
		{Labels: map[string]string{"a": "1"}, F: 5, T: 1000},
		{Labels: map[string]string{"a": "2"}, F: -1, T: 1000},
	}
	// Filter: > 0
	result := scalarBinaryOpVector(vec, 0, ">", false, false)
	if len(result) != 1 {
		t.Fatalf("expected 1 (only F=5 passes >0), got %d", len(result))
	}
	if result[0].F != 5 {
		t.Errorf("expected F=5, got %f", result[0].F)
	}
}

// === Resets / Changes tests ===

func TestResets(t *testing.T) {
	samples := []types.Sample{
		{Value: 1}, {Value: 5}, {Value: 3}, {Value: 6}, {Value: 2},
	}
	r := Resets(samples)
	if r != 2 {
		t.Errorf("resets = %f, expected 2", r)
	}
}

func TestChanges(t *testing.T) {
	samples := []types.Sample{
		{Value: 1}, {Value: 1}, {Value: 2}, {Value: 2}, {Value: 3},
	}
	c := Changes(samples)
	if c != 2 {
		t.Errorf("changes = %f, expected 2", c)
	}
}

// === Histogram quantile timestamp propagation ===

func TestApplyHistogramQuantile_Timestamp(t *testing.T) {
	// Simulate sum by(le)(rate(...)) output at evalTimeMs=5000
	vec := types.Vector{
		{Labels: map[string]string{"le": "0.1"}, F: 10, T: 5000},
		{Labels: map[string]string{"le": "0.5"}, F: 50, T: 5000},
		{Labels: map[string]string{"le": "1.0"}, F: 90, T: 5000},
		{Labels: map[string]string{"le": "+Inf"}, F: 100, T: 5000},
	}
	result := applyHistogramQuantile(vec, 0.5, 5000)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0].T != 5000 {
		t.Errorf("T = %d, expected 5000", result[0].T)
	}
	// p50: rank=50, bucket [0.1,0.5] has 40 items, result ≈ 0.5
	if result[0].F < 0.3 || result[0].F > 0.6 {
		t.Errorf("p50 = %f, expected ~0.5", result[0].F)
	}
	// Output labels should NOT contain "le"
	if _, has := result[0].Labels["le"]; has {
		t.Error("output should not contain 'le' label")
	}
}

func TestAggregateVector_PreservesTimestamp(t *testing.T) {
	vec := types.Vector{
		{Labels: map[string]string{"job": "a", "inst": "1"}, F: 10, T: 42000},
		{Labels: map[string]string{"job": "a", "inst": "2"}, F: 20, T: 42000},
	}
	result := AggregateVector(vec, "sum", []string{"job"}, false)
	if len(result) != 1 {
		t.Fatalf("expected 1 group, got %d", len(result))
	}
	if result[0].T != 42000 {
		t.Errorf("T = %d, expected 42000", result[0].T)
	}
}

func TestTopKNestedSum(t *testing.T) {
	// Simulate 3 jobs with multiple instances each
	vec := types.Vector{
		{Labels: map[string]string{"job": "api", "inst": "a"}, F: 10, T: 1000},
		{Labels: map[string]string{"job": "api", "inst": "b"}, F: 20, T: 1000},
		{Labels: map[string]string{"job": "web", "inst": "c"}, F: 50, T: 1000},
		{Labels: map[string]string{"job": "web", "inst": "d"}, F: 60, T: 1000},
		{Labels: map[string]string{"job": "db", "inst": "e"}, F: 5, T: 1000},
		{Labels: map[string]string{"job": "db", "inst": "f"}, F: 3, T: 1000},
	}

	// Step 1: sum by(job) → 3 groups: api=30, web=110, db=8
	summed := AggregateVector(vec, "sum", []string{"job"}, false)
	if len(summed) != 3 {
		t.Fatalf("sum by(job): expected 3 groups, got %d", len(summed))
	}

	// Step 2: topk(2, ...) → keep top 2 by value
	result := AggregateTopK(2, summed, nil, false, false)
	if len(result) != 2 {
		t.Fatalf("topk(2): expected 2 series, got %d", len(result))
	}

	// Verify labels are reduced (only "job", no "inst")
	for _, s := range result {
		if _, has := s.Labels["inst"]; has {
			t.Error("topk(sum by(job)) result should not have 'inst' label")
		}
		if _, has := s.Labels["job"]; !has {
			t.Error("topk(sum by(job)) result must have 'job' label")
		}
	}

	// Verify descending order (web=110 first, api=30 second)
	if result[0].F < result[1].F {
		t.Errorf("topk should return highest first: got %f, %f", result[0].F, result[1].F)
	}
	if result[0].F != 110 {
		t.Errorf("top1 = %f, expected 110 (web)", result[0].F)
	}
	if result[1].F != 30 {
		t.Errorf("top2 = %f, expected 30 (api)", result[1].F)
	}
}
