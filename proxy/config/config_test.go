package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func parseDuration(t *testing.T, s string) Duration {
	t.Helper()
	var d Duration
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(s), &node); err != nil {
		t.Fatal(err)
	}
	if err := d.UnmarshalYAML(node.Content[0]); err != nil {
		t.Fatal(err)
	}
	return d
}

// --- Duration ---

func TestDuration_Days(t *testing.T) {
	d := parseDuration(t, `"90d"`)
	if d.Duration != 90*24*time.Hour {
		t.Errorf("got %v, want 2160h", d.Duration)
	}
}

func TestDuration_Hours(t *testing.T) {
	d := parseDuration(t, `"2h"`)
	if d.Duration != 2*time.Hour {
		t.Errorf("got %v", d.Duration)
	}
}

func TestDuration_Minutes(t *testing.T) {
	d := parseDuration(t, `"5m"`)
	if d.Duration != 5*time.Minute {
		t.Errorf("got %v", d.Duration)
	}
}

func TestDuration_String_Days(t *testing.T) {
	d := Duration{Duration: 48 * time.Hour}
	if s := d.String(); s != "2d" {
		t.Errorf("got %q, want 2d", s)
	}
}

func TestDuration_String_NonDays(t *testing.T) {
	d := Duration{Duration: 5 * time.Hour}
	if s := d.String(); s != "5h0m0s" {
		t.Errorf("got %q", s)
	}
}

// --- DownsamplingConfig.Validate ---

func TestValidate_Disabled(t *testing.T) {
	d := &DownsamplingConfig{Enabled: false}
	if err := d.Validate(); err != nil {
		t.Errorf("disabled should be valid: %v", err)
	}
}

func TestValidate_NoTiers(t *testing.T) {
	d := &DownsamplingConfig{Enabled: true}
	if err := d.Validate(); err == nil {
		t.Error("expected error for no tiers")
	}
}

func TestValidate_RawRetentionTooShort(t *testing.T) {
	d := &DownsamplingConfig{
		Enabled:      true,
		RawRetention: Duration{Duration: 1 * time.Hour},
		Tiers: []TierConfig{{
			CompactAfter: Duration{Duration: 2 * time.Hour},
			MinStep:      Duration{Duration: time.Minute},
		}},
	}
	if err := d.Validate(); err == nil {
		t.Error("expected error: raw_retention < compact_after")
	}
}

func TestValidate_NonIncreasingCompactAfter(t *testing.T) {
	d := &DownsamplingConfig{
		Enabled:      true,
		RawRetention: Duration{Duration: 72 * time.Hour},
		Tiers: []TierConfig{
			{CompactAfter: Duration{Duration: 48 * time.Hour}, MinStep: Duration{Duration: time.Minute}},
			{CompactAfter: Duration{Duration: 24 * time.Hour}, MinStep: Duration{Duration: time.Hour}},
		},
	}
	if err := d.Validate(); err == nil {
		t.Error("expected error: non-increasing compact_after")
	}
}

func TestValidate_Valid(t *testing.T) {
	d := &DownsamplingConfig{
		Enabled:      true,
		RawRetention: Duration{Duration: 72 * time.Hour},
		Tiers: []TierConfig{
			{CompactAfter: Duration{Duration: 24 * time.Hour}, MinStep: Duration{Duration: time.Minute}},
			{CompactAfter: Duration{Duration: 48 * time.Hour}, MinStep: Duration{Duration: time.Hour}},
		},
	}
	if err := d.Validate(); err != nil {
		t.Errorf("valid config error: %v", err)
	}
}

// --- SelectTier ---

func TestSelectTier_Disabled(t *testing.T) {
	d := &DownsamplingConfig{Enabled: false}
	if tier := d.SelectTier(time.Minute); tier != nil {
		t.Error("disabled should return nil")
	}
}

func TestSelectTier_StepTooSmall(t *testing.T) {
	d := &DownsamplingConfig{
		Enabled: true,
		Tiers:   []TierConfig{{Name: "5m", MinStep: Duration{Duration: 5 * time.Minute}}},
	}
	if tier := d.SelectTier(time.Minute); tier != nil {
		t.Error("step < min_step should return nil")
	}
}

func TestSelectTier_ExactMinStep(t *testing.T) {
	d := &DownsamplingConfig{
		Enabled: true,
		Tiers:   []TierConfig{{Name: "5m", MinStep: Duration{Duration: 5 * time.Minute}}},
	}
	tier := d.SelectTier(5 * time.Minute)
	if tier == nil || tier.Name != "5m" {
		t.Error("step == min_step should match")
	}
}

func TestSelectTier_PicksCoarsest(t *testing.T) {
	d := &DownsamplingConfig{
		Enabled: true,
		Tiers: []TierConfig{
			{Name: "5m", MinStep: Duration{Duration: 5 * time.Minute}},
			{Name: "1h", MinStep: Duration{Duration: time.Hour}},
		},
	}
	tier := d.SelectTier(2 * time.Hour)
	if tier == nil || tier.Name != "1h" {
		t.Errorf("should pick 1h, got %v", tier)
	}
}

// --- QuerySegments ---

func TestQuerySegments_Disabled(t *testing.T) {
	d := &DownsamplingConfig{Enabled: false}
	now := time.Now()
	segs := d.QuerySegments(now.Add(-time.Hour), now, nil)
	if len(segs) != 1 || !segs[0].IsRaw {
		t.Error("disabled should return single raw segment")
	}
}

func TestQuerySegments_NilTier(t *testing.T) {
	d := &DownsamplingConfig{Enabled: true}
	now := time.Now()
	segs := d.QuerySegments(now.Add(-time.Hour), now, nil)
	if len(segs) != 1 || !segs[0].IsRaw {
		t.Error("nil tier should return single raw segment")
	}
}

func TestQuerySegments_HasRawTail(t *testing.T) {
	d := &DownsamplingConfig{
		Enabled: true,
		Tiers: []TierConfig{{
			Name: "5m", Table: "samples_5m",
			CompactAfter: Duration{Duration: 2 * time.Hour},
			Resolution:   Duration{Duration: 5 * time.Minute},
		}},
	}
	now := time.Now()
	tier := &d.Tiers[0]
	segs := d.QuerySegments(now.Add(-8*time.Hour), now, tier)
	hasRaw, hasTier := false, false
	for _, s := range segs {
		if s.IsRaw {
			hasRaw = true
		} else {
			hasTier = true
		}
	}
	if !hasRaw {
		t.Error("missing raw segment")
	}
	if !hasTier {
		t.Error("missing tier segment")
	}
}

// --- Load ---

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load("nonexistent_file_that_does_not_exist.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":9090" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.ClickHouse.Database != "metrics" {
		t.Errorf("Database = %q", cfg.ClickHouse.Database)
	}
}
