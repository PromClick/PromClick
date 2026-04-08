package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// MetricType represents the Prometheus metric type.
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
	MetricTypeUnknown   MetricType = "unknown"
)

// GetMetricType queries prom_metrics table for the metric type.
// Falls back to name-based inference if not found.
func (p *Pool) GetMetricType(ctx context.Context, metricName string) MetricType {
	var col proto.ColStr
	var metricType string
	err := p.pool.Do(ctx, ch.Query{
		Body: fmt.Sprintf(
			"SELECT metric_type FROM metrics.prom_metrics FINAL WHERE metric_name = '%s' LIMIT 1",
			strings.ReplaceAll(metricName, "'", "''"),
		),
		Result: proto.Results{{Name: "metric_type", Data: &col}},
		OnResult: func(_ context.Context, block proto.Block) error {
			if block.Rows > 0 {
				metricType = col.Row(0)
			}
			return nil
		},
	})
	if err == nil && metricType != "" {
		return MetricType(metricType)
	}
	return inferTypeFromName(metricName)
}

// IsCounterLike returns true if the metric is a counter (monotonically increasing).
func IsCounterLike(metricName string, mt MetricType) bool {
	if mt == MetricTypeCounter || mt == MetricTypeHistogram {
		return true // histogram buckets are cumulative counters
	}
	if mt != MetricTypeUnknown {
		return false
	}
	inferred := inferTypeFromName(metricName)
	return inferred == MetricTypeCounter || inferred == MetricTypeHistogram
}

// inferTypeFromName guesses metric type from naming conventions.
func inferTypeFromName(name string) MetricType {
	if strings.HasSuffix(name, "_total") ||
		strings.HasSuffix(name, "_count") ||
		strings.HasSuffix(name, "_sum") ||
		strings.HasSuffix(name, "_created") {
		return MetricTypeCounter
	}
	if strings.HasSuffix(name, "_bucket") {
		return MetricTypeHistogram
	}
	if strings.HasSuffix(name, "_info") {
		return MetricTypeGauge
	}
	return MetricTypeUnknown
}

// stripMetricSuffixes removes common Prometheus suffixes.
func stripMetricSuffixes(name string) string {
	suffixes := []string{"_total", "_count", "_sum", "_bucket", "_created", "_info"}
	for _, s := range suffixes {
		if strings.HasSuffix(name, s) {
			return name[:len(name)-len(s)]
		}
	}
	return name
}
