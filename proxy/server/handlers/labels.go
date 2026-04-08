package handlers

import (
	"net/http"
)

// Labels handles /api/v1/labels.
func (h *Handler) Labels(w http.ResponseWriter, r *http.Request) {
	labels, err := h.Meta.Labels(
		r.Context(),
		h.PromCfg.Schema.TimeSeriesTable,
		h.PromCfg.Schema.Columns.Labels,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err)
		return
	}
	writeJSON(w, APIResponse{
		Status: "success",
		Data:   labels,
	})
}

// LabelValues handles /api/v1/label/{name}/values.
func (h *Handler) LabelValues(w http.ResponseWriter, r *http.Request) {
	labelName := r.PathValue("name")
	if labelName == "" {
		writeError(w, http.StatusBadRequest, "bad_data", "missing label name")
		return
	}

	values, err := h.Meta.LabelValues(
		r.Context(),
		labelName,
		h.PromCfg.Schema.TimeSeriesTable,
		h.PromCfg.Schema.Columns.MetricName,
		h.PromCfg.Schema.Columns.Labels,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err)
		return
	}
	if values == nil {
		values = []string{}
	}
	writeJSON(w, APIResponse{
		Status: "success",
		Data:   values,
	})
}

// TSDBStatus handles /api/v1/status/tsdb.
func (h *Handler) TSDBStatus(w http.ResponseWriter, r *http.Request) {
	status, err := h.Meta.TSDBStatus(
		r.Context(),
		h.PromCfg.Schema.TimeSeriesTable,
		h.PromCfg.Schema.Columns.MetricName,
		h.PromCfg.Schema.Columns.Labels,
		h.PromCfg.Schema.SamplesTable,
		h.PromCfg.Schema.Columns.Timestamp,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err)
		return
	}
	writeJSON(w, APIResponse{
		Status: "success",
		Data:   status,
	})
}
