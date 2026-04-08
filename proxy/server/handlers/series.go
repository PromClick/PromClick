package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// Series handles /api/v1/series — returns label sets matching match[] selectors.
func (h *Handler) Series(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

	matchers := r.Form["match[]"]
	if len(matchers) == 0 {
		writeJSON(w, APIResponse{Status: "success", Data: []interface{}{}})
		return
	}

	// For simplicity, extract metric name from first matcher and return label sets from CH
	// match[] typically looks like: {__name__="metric"} or metric_name
	results, err := h.Meta.Series(r.Context(), matchers,
		h.PromCfg.Schema.TimeSeriesTable,
		h.PromCfg.Schema.Columns.MetricName,
		h.PromCfg.Schema.Columns.Labels,
	)
	if err != nil {
		writeJSON(w, APIResponse{Status: "success", Data: []interface{}{}})
		return
	}

	writeJSON(w, APIResponse{Status: "success", Data: results})
}

// Series on MetaQuerier — query CH for label sets matching a metric.
func (m *MetaQuerier) Series(ctx context.Context, matchers []string, tagsTable, metricNameCol, labelsCol string) ([]map[string]string, error) {
	// Extract metric name from matcher like "{__name__=\"up\"}" or "up" or "{__name__=~\"node_.*\"}"
	metricName := ""
	for _, match := range matchers {
		// Try simple metric name (no braces)
		if len(match) > 0 && match[0] != '{' {
			metricName = match
			break
		}
		// Try {__name__="value"} pattern
		// Simple regex-free extraction
		for i := 0; i < len(match)-1; i++ {
			if match[i] == '"' {
				end := i + 1
				for end < len(match) && match[end] != '"' {
					end++
				}
				if end < len(match) {
					metricName = match[i+1 : end]
					break
				}
			}
		}
		if metricName != "" {
			break
		}
	}

	if metricName == "" {
		return nil, fmt.Errorf("no metric name found in matchers")
	}

	sql := fmt.Sprintf(
		"SELECT %s AS metric_name, %s AS labels FROM %s WHERE %s = '%s' ORDER BY unix_milli DESC LIMIT 1 BY fingerprint LIMIT 100",
		metricNameCol, labelsCol, tagsTable, metricNameCol, metricName,
	)

	rows, err := m.query(ctx, sql)
	if err != nil {
		return nil, err
	}

	var results []map[string]string
	for _, row := range rows {
		var v struct {
			MetricName string            `json:"metric_name"`
			Labels     map[string]string `json:"labels"`
		}
		if err := json.Unmarshal(row, &v); err != nil {
			continue
		}
		labelSet := make(map[string]string)
		labelSet["__name__"] = v.MetricName
		for k, val := range v.Labels {
			labelSet[k] = val
		}
		results = append(results, labelSet)
	}
	return results, nil
}
