package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// MetaQuerier performs metadata queries against ClickHouse HTTP interface.
type MetaQuerier struct {
	Addr       string
	Database   string
	User       string
	Password   string
	HTTPClient *http.Client
}

func (m *MetaQuerier) query(ctx context.Context, sql string) ([][]byte, error) {
	u := strings.TrimRight(m.Addr, "/") + "/?database=" + m.Database + "&default_format=JSONEachRow"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	if m.User != "" {
		req.SetBasicAuth(m.User, m.Password)
	}
	req.Header.Set("Content-Type", "text/plain")

	client := m.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("clickhouse HTTP %d: %s", resp.StatusCode, string(body))
	}

	var rows [][]byte
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, err
		}
		rows = append(rows, raw)
	}
	return rows, nil
}

// Labels returns all distinct label names from the tags table.
func (m *MetaQuerier) Labels(ctx context.Context, tagsTable, labelsCol string) ([]string, error) {
	sql := fmt.Sprintf("SELECT DISTINCT arrayJoin(JSONExtractKeys(%s)) AS name FROM %s ORDER BY name",
		labelsCol, tagsTable)
	rows, err := m.query(ctx, sql)
	if err != nil {
		return nil, err
	}
	var result []string
	for _, row := range rows {
		var v struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(row, &v); err == nil && v.Name != "" {
			result = append(result, v.Name)
		}
	}
	// Always include __name__
	found := false
	for _, l := range result {
		if l == "__name__" {
			found = true
			break
		}
	}
	if !found {
		result = append([]string{"__name__"}, result...)
	}
	return result, nil
}

// LabelValues returns distinct values for a given label.
func (m *MetaQuerier) LabelValues(ctx context.Context, labelName, tagsTable, metricNameCol, labelsCol string) ([]string, error) {
	var sql string
	if labelName == "__name__" {
		sql = fmt.Sprintf("SELECT DISTINCT %s AS value FROM %s ORDER BY value",
			metricNameCol, tagsTable)
	} else {
		sql = fmt.Sprintf("SELECT DISTINCT JSONExtractString(%s, '%s') AS value FROM %s WHERE value != '' ORDER BY value",
			labelsCol, labelName, tagsTable)
	}
	rows, err := m.query(ctx, sql)
	if err != nil {
		return nil, err
	}
	var result []string
	for _, row := range rows {
		var v struct {
			Value string `json:"value"`
		}
		if err := json.Unmarshal(row, &v); err == nil && v.Value != "" {
			result = append(result, v.Value)
		}
	}
	return result, nil
}

// parseCount extracts a count value from CH JSON row (count() returns string in JSONEachRow).
func parseCount(row []byte) float64 {
	var v struct {
		C json.Number `json:"c"`
	}
	json.Unmarshal(row, &v)
	f, _ := v.C.Float64()
	return f
}

// TSDBStatus returns cardinality stats from ClickHouse.
func (m *MetaQuerier) TSDBStatus(ctx context.Context, tagsTable, metricNameCol, labelsCol, samplesTable, tsCol string) (map[string]interface{}, error) {
	// Total series (count distinct fingerprints)
	rows, _ := m.query(ctx, fmt.Sprintf("SELECT count(DISTINCT fingerprint) AS c FROM %s", tagsTable))
	var numSeries float64
	if len(rows) > 0 {
		numSeries = parseCount(rows[0])
	}

	// Total samples
	rows, _ = m.query(ctx, fmt.Sprintf("SELECT count() AS c FROM %s", samplesTable))
	var numSamples float64
	if len(rows) > 0 {
		numSamples = parseCount(rows[0])
	}

	// Top 10 metrics by series count
	rows, _ = m.query(ctx, fmt.Sprintf(
		"SELECT %s AS name, count(DISTINCT fingerprint) AS c FROM %s GROUP BY name ORDER BY c DESC LIMIT 10",
		metricNameCol, tagsTable))
	var topMetrics []map[string]interface{}
	for _, row := range rows {
		var v struct {
			Name string      `json:"name"`
			C    json.Number `json:"c"`
		}
		json.Unmarshal(row, &v)
		cnt, _ := v.C.Float64()
		topMetrics = append(topMetrics, map[string]interface{}{"name": v.Name, "seriesCount": cnt})
	}

	// Top 10 label names by value count
	rows, _ = m.query(ctx, fmt.Sprintf(
		"SELECT name, count() AS c FROM (SELECT DISTINCT arrayJoin(JSONExtractKeys(%s)) AS name FROM %s) GROUP BY name ORDER BY c DESC LIMIT 10",
		labelsCol, tagsTable))
	var topLabels []map[string]interface{}
	for _, row := range rows {
		var v struct {
			Name string      `json:"name"`
			C    json.Number `json:"c"`
		}
		json.Unmarshal(row, &v)
		cnt, _ := v.C.Float64()
		topLabels = append(topLabels, map[string]interface{}{"name": v.Name, "seriesCount": cnt})
	}

	return map[string]interface{}{
		"numSeries":     numSeries,
		"numSamples":    numSamples,
		"topMetrics":    topMetrics,
		"topLabelNames": topLabels,
	}, nil
}
