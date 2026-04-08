package handlers

import (
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// StatusHandler holds health/readiness state.
type StatusHandler struct {
	IsReady   *atomic.Bool
	IsHealthy *atomic.Bool
	StartTime time.Time
}

// NewStatusHandler creates a new StatusHandler with healthy=true, ready=false.
func NewStatusHandler() *StatusHandler {
	h := &StatusHandler{
		IsReady:   &atomic.Bool{},
		IsHealthy: &atomic.Bool{},
		StartTime: time.Now(),
	}
	h.IsHealthy.Store(true)
	return h
}

// Healthy returns 200 if healthy.
func (s *StatusHandler) Healthy(w http.ResponseWriter, r *http.Request) {
	if s.IsHealthy.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Healthy\n"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("Unhealthy\n"))
}

// Ready returns 200 if ready.
func (s *StatusHandler) Ready(w http.ResponseWriter, r *http.Request) {
	if s.IsReady.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ready\n"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("Not Ready\n"))
}

// RuntimeInfo returns stub runtime info.
func (s *StatusHandler) RuntimeInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, APIResponse{
		Status: "success",
		Data: map[string]interface{}{
			"startTime":           s.StartTime.Format(time.RFC3339),
			"CWD":                 "",
			"reloadConfigSuccess": true,
			"lastConfigTime":      s.StartTime.Format(time.RFC3339),
			"corruptionCount":     0,
			"goroutineCount":      runtime.NumGoroutine(),
			"GOMAXPROCS":          runtime.GOMAXPROCS(0),
			"GOGC":                "",
			"GODEBUG":             "",
			"storageRetention":    "7d",
		},
	})
}

// StatusConfig returns the running configuration as YAML.
func (h *Handler) StatusConfig(w http.ResponseWriter, r *http.Request) {
	yamlBytes, err := yaml.Marshal(h.Cfg)
	yamlStr := ""
	if err == nil {
		yamlStr = string(yamlBytes)
	}
	writeJSON(w, APIResponse{
		Status: "success",
		Data: map[string]interface{}{
			"yaml": yamlStr,
		},
	})
}

// StatusFlags returns stub flags info.
func StatusFlags(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, APIResponse{
		Status: "success",
		Data:   map[string]string{},
	})
}
