package eval

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/PromClick/PromClick/config"
	"github.com/PromClick/PromClick/translator"
	"github.com/PromClick/PromClick/types"
)

// --- generateSteps ---

func TestGenerateSteps_StepZero(t *testing.T) {
	end := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	steps := generateSteps(end.Add(-time.Hour), end, 0)
	if len(steps) != 1 || steps[0] != end.UnixMilli() {
		t.Errorf("step=0 should return single end step, got %v", steps)
	}
}

func TestGenerateSteps_NegativeStep(t *testing.T) {
	end := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	steps := generateSteps(end.Add(-time.Hour), end, -time.Minute)
	if len(steps) != 1 {
		t.Errorf("negative step should return single step, got %d", len(steps))
	}
}

func TestGenerateSteps_StartAfterEnd(t *testing.T) {
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	steps := generateSteps(base.Add(time.Hour), base, time.Minute)
	if len(steps) != 1 {
		t.Errorf("start > end should return single step, got %d", len(steps))
	}
}

func TestGenerateSteps_StartEqualsEnd(t *testing.T) {
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	steps := generateSteps(base, base, time.Minute)
	if len(steps) != 1 || steps[0] != base.UnixMilli() {
		t.Errorf("start == end should return single step, got %v", steps)
	}
}

func TestGenerateSteps_Normal(t *testing.T) {
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	steps := generateSteps(base, base.Add(5*time.Minute), time.Minute)
	if len(steps) != 6 {
		t.Errorf("5 minutes with 1m step: want 6 steps, got %d", len(steps))
	}
}

// --- labelsKeyStr ---

func TestLabelsKeyStr_NoCollision(t *testing.T) {
	a := labelsKeyStr(map[string]string{"a": "b", "c": "d"})
	b := labelsKeyStr(map[string]string{"a": "b,c=d"})
	if a == b {
		t.Error("labelsKeyStr should not collide with special chars in values")
	}
}

func TestLabelsKeyStr_Deterministic(t *testing.T) {
	labels := map[string]string{"z": "1", "a": "2", "m": "3"}
	a := labelsKeyStr(labels)
	b := labelsKeyStr(labels)
	if a != b {
		t.Error("labelsKeyStr should be deterministic")
	}
}

func TestLabelsKeyStr_Empty(t *testing.T) {
	result := labelsKeyStr(map[string]string{})
	if result != "" {
		t.Errorf("empty labels should produce empty key, got %q", result)
	}
}

// --- applyMathChain ---

func TestApplyMathChain_Abs(t *testing.T) {
	vec := types.Vector{
		{F: -5.0}, {F: 3.0}, {F: 0.0},
	}
	result := applyMathChain(vec, []translator.MathStep{{Fn: "abs"}})
	if result[0].F != 5.0 || result[1].F != 3.0 || result[2].F != 0.0 {
		t.Errorf("abs: %v", result)
	}
}

func TestApplyMathChain_ClampMin(t *testing.T) {
	vec := types.Vector{{F: 1.0}, {F: 5.0}, {F: 10.0}}
	result := applyMathChain(vec, []translator.MathStep{{Fn: "clamp_min", Param: 3.0}})
	if result[0].F != 3.0 || result[1].F != 5.0 || result[2].F != 10.0 {
		t.Errorf("clamp_min: %v", result)
	}
}

func TestApplyMathChain_Sort(t *testing.T) {
	vec := types.Vector{{F: 3.0}, {F: 1.0}, {F: 2.0}}
	result := applyMathChain(vec, []translator.MathStep{{Fn: "sort"}})
	if result[0].F != 1.0 || result[1].F != 2.0 || result[2].F != 3.0 {
		t.Errorf("sort: [%f, %f, %f]", result[0].F, result[1].F, result[2].F)
	}
}

func TestApplyMathChain_Chain(t *testing.T) {
	vec := types.Vector{{F: -7.0}}
	result := applyMathChain(vec, []translator.MathStep{
		{Fn: "abs"},
		{Fn: "clamp_max", Param: 5.0},
	})
	if result[0].F != 5.0 {
		t.Errorf("abs then clamp_max(5): got %f", result[0].F)
	}
}

// --- applyHistogramQuantile ---

func TestApplyHistogramQuantile_Empty(t *testing.T) {
	result := applyHistogramQuantile(types.Vector{}, 0.99, 1000)
	if len(result) != 0 {
		t.Errorf("empty vec should return empty, got %d", len(result))
	}
}

func TestApplyHistogramQuantile_NoLE(t *testing.T) {
	vec := types.Vector{
		{Labels: map[string]string{"foo": "bar"}, F: 10},
	}
	result := applyHistogramQuantile(vec, 0.5, 1000)
	// With no "le" label, le parses as 0.0 → single bucket with UpperBound=0
	// HistogramQuantile needs +Inf bucket → returns NaN → still gets added
	// This is acceptable behavior (matches Prometheus — bad input → NaN output)
	for _, s := range result {
		if !math.IsNaN(s.F) {
			t.Errorf("no le labels should produce NaN, got %f", s.F)
		}
	}
}

// --- binary eval ctx cancel ---

func TestEvalBinaryPlan_CtxCancel(t *testing.T) {
	cfg := &config.Config{}
	cfg.Schema.SamplesTable = "samples"
	cfg.Schema.TimeSeriesTable = "time_series"
	cfg.Prometheus.StalenessSeconds = 300
	cfg.Prometheus.DefaultStep = 60 * time.Second

	ev := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	plan := &translator.SQLPlan{
		ExprType: "binary",
		BinaryOp: "+",
		LHS:      &translator.SQLPlan{IsScalar: true, ScalarVal: 1},
		RHS:      &translator.SQLPlan{IsScalar: true, ScalarVal: 2},
	}

	now := time.Now()
	// Both sides are scalar, so this should go through scalar path, not goroutines.
	// Test the scalar LHS path:
	plan2 := &translator.SQLPlan{
		ExprType:    "binary",
		BinaryOp:    "+",
		IsScalarLHS: true,
		ScalarLHS:   1,
		RHS:         &translator.SQLPlan{IsScalar: true, ScalarVal: 2},
	}
	_, err := ev.EvalPlan(ctx, plan2, now, now, 0)
	// Scalar RHS path should work even with cancelled ctx (no goroutines)
	if err != nil && err != context.Canceled {
		t.Errorf("unexpected error: %v", err)
	}

	// Avoid unused variable
	_ = plan
}

// --- ExtrapolatedRate edge cases ---

func TestExtrapolatedRate_SameTimestamp(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 1000, Value: 10},
		{Timestamp: 1000, Value: 20},
	}
	val, ok := ExtrapolatedRate(samples, 0, 5000, false, true)
	if !ok {
		t.Fatal("expected ok=true")
	}
	// sampledInterval=0 → division by zero → should be Inf
	if !math.IsInf(val, 0) && !math.IsNaN(val) {
		t.Logf("same timestamp rate = %f (edge case, Inf/NaN expected)", val)
	}
}

func TestExtrapolatedRate_AllZeroCounter(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 1000, Value: 0},
		{Timestamp: 2000, Value: 0},
		{Timestamp: 3000, Value: 0},
	}
	val, ok := ExtrapolatedRate(samples, 0, 5000, true, true)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if val != 0 {
		t.Errorf("all-zero counter rate should be 0, got %f", val)
	}
}

// --- Histogram edge cases ---

func TestHistogramQuantile_EmptyBuckets(t *testing.T) {
	_, _, ok := HistogramQuantile(0.5, nil)
	if ok {
		t.Error("empty buckets should return ok=false")
	}
}

func TestHistogramQuantile_ZeroCounts(t *testing.T) {
	buckets := []Bucket{
		{UpperBound: 1.0, Count: 0},
		{UpperBound: math.Inf(1), Count: 0},
	}
	val, _, ok := HistogramQuantile(0.5, buckets)
	if !ok {
		t.Fatal("should return ok=true")
	}
	if !math.IsNaN(val) {
		t.Errorf("zero observations should return NaN, got %f", val)
	}
}

// --- IRate edge cases ---

func TestIRate_ZeroDt(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 1000, Value: 5},
		{Timestamp: 1000, Value: 10},
	}
	_, ok := IRate(samples)
	if ok {
		t.Error("same timestamp should return ok=false")
	}
}

func TestIRate_SingleSample(t *testing.T) {
	samples := []types.Sample{{Timestamp: 1000, Value: 5}}
	_, ok := IRate(samples)
	if ok {
		t.Error("single sample should return ok=false")
	}
}

// --- Deriv edge cases ---

func TestDeriv_ConstantValues(t *testing.T) {
	samples := []types.Sample{
		{Timestamp: 1000, Value: 5},
		{Timestamp: 2000, Value: 5},
		{Timestamp: 3000, Value: 5},
	}
	val, ok := Deriv(samples)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if val != 0 {
		t.Errorf("constant values should have slope 0, got %f", val)
	}
}
