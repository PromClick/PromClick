package handlers

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hinskii/promclick/types"
)

// APIResponse is the standard Prometheus API response envelope.
type APIResponse struct {
	Status    string      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType string      `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writeJSON: encode failed", "error", err)
	}
}

// writeMatrixResponse streams a matrix result directly to the writer,
// avoiding intermediate []interface{} allocations for large result sets.
func writeMatrixResponse(w http.ResponseWriter, matrix types.Matrix) {
	w.Header().Set("Content-Type", "application/json")

	bw := bufio.NewWriterSize(w, 64*1024)

	bw.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)

	for si, s := range matrix {
		if si > 0 {
			bw.WriteByte(',')
		}
		bw.WriteString(`{"metric":`)
		lb, _ := json.Marshal(s.Labels)
		bw.Write(lb)
		bw.WriteString(`,"values":[`)

		for vi, p := range s.Samples {
			if vi > 0 {
				bw.WriteByte(',')
			}
			bw.WriteByte('[')
			// timestamp as float seconds
			ts := strconv.FormatFloat(float64(p.Timestamp)/1000.0, 'f', 3, 64)
			bw.WriteString(ts)
			bw.WriteString(`,"`)
			bw.WriteString(formatFloat(p.Value))
			bw.WriteString(`"]`)
		}
		bw.WriteString(`]}`)
	}

	bw.WriteString(`]}}`)
	bw.WriteByte('\n')
	if err := bw.Flush(); err != nil {
		slog.Error("writeMatrixResponse: flush failed", "error", err)
	}
}

func writeError(w http.ResponseWriter, code int, errType string, err interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if encErr := json.NewEncoder(w).Encode(APIResponse{
		Status:    "error",
		ErrorType: errType,
		Error:     fmt.Sprint(err),
	}); encErr != nil {
		slog.Error("writeError: encode failed", "error", encErr)
	}
}

// parsePrometheusTime handles unix float, RFC3339, and empty string (now).
func parsePrometheusTime(s string) (time.Time, error) {
	if s == "" {
		return time.Now(), nil
	}

	// Try unix float timestamp
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		sec, frac := math.Modf(f)
		return time.Unix(int64(sec), int64(frac*1e9)).UTC(), nil
	}

	// Try RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("cannot parse %q as Prometheus time", s)
}

// parsePrometheusDuration handles Go duration strings and float seconds.
func parsePrometheusDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	// Try Go duration string (e.g., "60s", "2m")
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// Try as float seconds
	s = strings.TrimSpace(s)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(f * float64(time.Second)), nil
	}

	return 0, fmt.Errorf("cannot parse %q as duration", s)
}

// formOrQuery reads a parameter from form data or query string.
func formOrQuery(r *http.Request, key string) string {
	if v := r.FormValue(key); v != "" {
		return v
	}
	return r.URL.Query().Get(key)
}
