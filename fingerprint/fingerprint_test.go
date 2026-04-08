package fingerprint

import "testing"

func TestCompute_Deterministic(t *testing.T) {
	labels := map[string]string{"job": "api", "instance": "localhost:9090"}
	a := Compute(labels)
	b := Compute(labels)
	if a != b {
		t.Errorf("non-deterministic: %d != %d", a, b)
	}
}

func TestCompute_OrderIndependent(t *testing.T) {
	a := Compute(map[string]string{"a": "1", "b": "2", "c": "3"})
	b := Compute(map[string]string{"c": "3", "a": "1", "b": "2"})
	if a != b {
		t.Errorf("order-dependent: %d != %d", a, b)
	}
}

func TestCompute_DifferentLabels_DifferentHash(t *testing.T) {
	a := Compute(map[string]string{"job": "api"})
	b := Compute(map[string]string{"job": "web"})
	if a == b {
		t.Errorf("collision: both = %d", a)
	}
}

func TestCompute_EmptyLabels(t *testing.T) {
	fp := Compute(map[string]string{})
	if fp == 0 {
		t.Error("empty labels should produce non-zero fingerprint")
	}
}

func TestCompute_KeyValueSeparation(t *testing.T) {
	// "a"="bc" vs "ab"="c" must differ
	a := Compute(map[string]string{"a": "bc"})
	b := Compute(map[string]string{"ab": "c"})
	if a == b {
		t.Errorf("key-value boundary collision: both = %d", a)
	}
}
