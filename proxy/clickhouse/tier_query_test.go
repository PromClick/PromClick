package clickhouse

import (
	"strings"
	"testing"
)

func TestUintSliceToSQL_Empty(t *testing.T) {
	if s := uintSliceToSQL(nil); s != "" {
		t.Errorf("got %q, want empty", s)
	}
}

func TestUintSliceToSQL_Single(t *testing.T) {
	s := uintSliceToSQL([]uint64{12345})
	if s != "12345" {
		t.Errorf("got %q", s)
	}
}

func TestUintSliceToSQL_Multiple(t *testing.T) {
	s := uintSliceToSQL([]uint64{1, 2, 3})
	if s != "1,2,3" {
		t.Errorf("got %q", s)
	}
}

func TestUintSliceToSQL_Large(t *testing.T) {
	s := uintSliceToSQL([]uint64{18446744073709551615}) // max uint64
	if s != "18446744073709551615" {
		t.Errorf("got %q", s)
	}
}

func TestUintSliceToSQL_NoSpaces(t *testing.T) {
	s := uintSliceToSQL([]uint64{100, 200, 300})
	if strings.Contains(s, " ") {
		t.Errorf("should not contain spaces: %q", s)
	}
}
