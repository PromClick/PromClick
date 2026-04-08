package handlers

import (
	"net/http"
	"runtime"
)

// BuildInfo returns Prometheus-compatible build information.
// Version >= 2.54.0 is required for Grafana compatibility.
func BuildInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, APIResponse{
		Status: "success",
		Data: map[string]string{
			"version":   "2.54.0",
			"revision":  "promql-ch-proxy",
			"branch":    "main",
			"buildUser": "",
			"buildDate": "",
			"goVersion": runtime.Version(),
		},
	})
}
