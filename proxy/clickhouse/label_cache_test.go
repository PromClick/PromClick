package clickhouse

import (
	"testing"
)

func newTestCache() *LabelCache {
	c := &LabelCache{
		data:        make(map[string]map[string]string),
		metricIndex: make(map[string][]string),
		ttl:         60,
		maxSeries:   10000,
		loaded:      true,
	}
	return c
}

func (c *LabelCache) setForTest(fp, metricName string, labels map[string]string) {
	c.data[fp] = labels
	c.metricIndex[metricName] = append(c.metricIndex[metricName], fp)
}

func TestLabelCache_UInt64Fingerprint(t *testing.T) {
	c := newTestCache()
	c.setForTest("12345678", "test_metric", map[string]string{"job": "api", "instance": "host-1"})

	labels, ok := c.GetLabels("12345678")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if labels["job"] != "api" {
		t.Errorf("job=%s, want api", labels["job"])
	}

	fps, ok := c.GetFingerprints("test_metric", []LabelMatcher{{Name: "job", Op: "=", Value: "api"}})
	if !ok || len(fps) != 1 || fps[0] != "12345678" {
		t.Errorf("GetFingerprints = %v, want [12345678]", fps)
	}
}

func TestLabelCache_UUIDFingerprint(t *testing.T) {
	uuid := "550e8400-e29b-41d4-a716-446655440000"
	c := newTestCache()
	c.setForTest(uuid, "test_metric", map[string]string{"job": "web", "dc": "us-east"})

	labels, ok := c.GetLabels(uuid)
	if !ok {
		t.Fatal("expected cache hit for UUID")
	}
	if labels["dc"] != "us-east" {
		t.Errorf("dc=%s, want us-east", labels["dc"])
	}

	// != filter
	fps, ok := c.GetFingerprints("test_metric", []LabelMatcher{{Name: "job", Op: "!=", Value: "api"}})
	if !ok || len(fps) != 1 {
		t.Errorf("!= filter: got %d fps, want 1", len(fps))
	}
}

func TestLabelCache_NegativeRegex(t *testing.T) {
	c := newTestCache()
	c.setForTest("fp1", "cpu", map[string]string{"mode": "idle"})
	c.setForTest("fp2", "cpu", map[string]string{"mode": "user"})
	c.setForTest("fp3", "cpu", map[string]string{"mode": "system"})
	c.setForTest("fp4", "cpu", map[string]string{"mode": "iowait"})

	fps, ok := c.GetFingerprints("cpu", []LabelMatcher{{Name: "mode", Op: "!~", Value: "idle|iowait"}})
	if !ok {
		t.Fatal("expected ok")
	}
	if len(fps) != 2 {
		t.Errorf("!~ filter: got %d fps, want 2 (user, system)", len(fps))
	}
}

func TestLabelCache_EmptyEquals(t *testing.T) {
	c := newTestCache()
	c.setForTest("fp1", "up", map[string]string{"instance": "host:9100"})

	// instance="" should match nothing (instance is non-empty)
	fps, ok := c.GetFingerprints("up", []LabelMatcher{{Name: "instance", Op: "=", Value: ""}})
	if !ok {
		t.Fatal("expected ok")
	}
	if len(fps) != 0 {
		t.Errorf("instance=\"\" should return 0, got %d", len(fps))
	}
}

func TestLabelCache_MaxSeries(t *testing.T) {
	c := newTestCache()
	c.maxSeries = 2
	c.setForTest("fp1", "big_metric", map[string]string{"i": "1"})
	c.setForTest("fp2", "big_metric", map[string]string{"i": "2"})
	c.setForTest("fp3", "big_metric", map[string]string{"i": "3"})

	_, ok := c.GetFingerprints("big_metric", nil)
	if ok {
		t.Error("expected fallback (ok=false) when > maxSeries")
	}
}

func TestBuildFingerprintIN_UInt64(t *testing.T) {
	result := buildFingerprintIN([]string{"123456", "789012"}, "fingerprint")
	expected := "AND fingerprint IN (123456,789012)"
	if result != expected {
		t.Errorf("got %q, want %q", result, expected)
	}
}


func TestBuildFingerprintIN_Empty(t *testing.T) {
	result := buildFingerprintIN(nil, "fp")
	if result != "" {
		t.Errorf("empty fps should return empty string, got %q", result)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
