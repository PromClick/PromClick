package handlers

import (
	"math"
	"testing"
	"time"
)

// --- parsePrometheusTime ---

func TestParsePrometheusTime_Empty(t *testing.T) {
	tm, err := parsePrometheusTime("")
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(tm) > 2*time.Second {
		t.Error("empty should return ~now")
	}
}

func TestParsePrometheusTime_UnixFloat(t *testing.T) {
	tm, err := parsePrometheusTime("1704067200.123")
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2024, 1, 1, 0, 0, 0, 123000000, time.UTC)
	if tm.Sub(expected).Abs() > time.Millisecond {
		t.Errorf("got %v, want ~%v", tm, expected)
	}
}

func TestParsePrometheusTime_RFC3339(t *testing.T) {
	tm, err := parsePrometheusTime("2024-01-01T12:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	if !tm.Equal(expected) {
		t.Errorf("got %v, want %v", tm, expected)
	}
}

func TestParsePrometheusTime_Invalid(t *testing.T) {
	_, err := parsePrometheusTime("not-a-time")
	if err == nil {
		t.Error("expected error for invalid time")
	}
}

// --- parsePrometheusDuration ---

func TestParsePrometheusDuration_GoFormat(t *testing.T) {
	d, err := parsePrometheusDuration("5m")
	if err != nil {
		t.Fatal(err)
	}
	if d != 5*time.Minute {
		t.Errorf("got %v", d)
	}
}

func TestParsePrometheusDuration_FloatSeconds(t *testing.T) {
	d, err := parsePrometheusDuration("60")
	if err != nil {
		t.Fatal(err)
	}
	if d != 60*time.Second {
		t.Errorf("got %v", d)
	}
}

func TestParsePrometheusDuration_Empty(t *testing.T) {
	_, err := parsePrometheusDuration("")
	if err == nil {
		t.Error("expected error for empty")
	}
}

func TestParsePrometheusDuration_FloatFraction(t *testing.T) {
	d, err := parsePrometheusDuration("0.5")
	if err != nil {
		t.Fatal(err)
	}
	if d != 500*time.Millisecond {
		t.Errorf("got %v", d)
	}
}

// --- formatFloat ---

func TestFormatFloat_NaN(t *testing.T) {
	if s := formatFloat(math.NaN()); s != "NaN" {
		t.Errorf("got %q", s)
	}
}

func TestFormatFloat_PosInf(t *testing.T) {
	if s := formatFloat(math.Inf(1)); s != "+Inf" {
		t.Errorf("got %q", s)
	}
}

func TestFormatFloat_NegInf(t *testing.T) {
	if s := formatFloat(math.Inf(-1)); s != "-Inf" {
		t.Errorf("got %q", s)
	}
}

func TestFormatFloat_Normal(t *testing.T) {
	if s := formatFloat(3.14); s != "3.14" {
		t.Errorf("got %q", s)
	}
}

func TestFormatFloat_Integer(t *testing.T) {
	if s := formatFloat(42.0); s != "42" {
		t.Errorf("got %q", s)
	}
}

func TestFormatFloat_Zero(t *testing.T) {
	if s := formatFloat(0.0); s != "0" {
		t.Errorf("got %q", s)
	}
}
