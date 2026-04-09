package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/prometheus/prometheus/prompb"

	"github.com/PromClick/PromClick/fingerprint"
)

// WriterConfig holds configuration for the batch writer.
type WriterConfig struct {
	Database       string
	BatchSize      int
	QueueSize      int
	FlushInterval  time.Duration
}

// Writer batches incoming remote_write data and flushes to ClickHouse.
type Writer struct {
	pool   *Pool
	cfg    WriterConfig
	queue  chan writeBatch
}

type writeBatch struct {
	series []prompb.TimeSeries
}

// NewWriter creates a Writer with a background flush goroutine.
func NewWriter(pool *Pool, cfg WriterConfig) *Writer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10000
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 100000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	return &Writer{
		pool:  pool,
		cfg:   cfg,
		queue: make(chan writeBatch, cfg.QueueSize),
	}
}

// Start begins the background flush goroutine.
func (w *Writer) Start(ctx context.Context) {
	go w.run(ctx)
}

// Write enqueues a write request for batched insertion.
func (w *Writer) Write(ctx context.Context, req *prompb.WriteRequest) error {
	select {
	case w.queue <- writeBatch{series: req.Timeseries}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) run(ctx context.Context) {
	batch := make([]prompb.TimeSeries, 0, w.cfg.BatchSize)
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case b := <-w.queue:
			batch = append(batch, b.series...)
			if len(batch) >= w.cfg.BatchSize {
				w.flush(ctx, batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				w.flush(ctx, batch)
				batch = batch[:0]
			}
		case <-ctx.Done():
			// Drain remaining
			if len(batch) > 0 {
				w.flush(context.Background(), batch)
			}
			return
		}
	}
}

type sampleRow struct {
	fingerprint uint64
	metricName  string
	unixMilli   int64
	value       float64
}

type seriesRow struct {
	fingerprint uint64
	metricName  string
	labels      map[string]string
	unixMilli   int64
}

func (w *Writer) flush(ctx context.Context, series []prompb.TimeSeries) {
	t0 := time.Now()

	// Deduplicate series metadata and collect all samples
	seenSeries := make(map[string]bool)
	var seriesRows []seriesRow
	var sampleRows []sampleRow

	for i := range series {
		ts := &series[i]
		labels := make(map[string]string, len(ts.Labels))
		metricName := ""
		for _, l := range ts.Labels {
			if l.Name == "__name__" {
				metricName = l.Value
			} else {
				labels[l.Name] = l.Value
			}
		}

		fp := fingerprint.Compute(labels)

		// Time-bucketing: use first sample timestamp, rounded to hour
		const hourMs int64 = 3_600_000
		var bucketMilli int64
		if len(ts.Samples) > 0 {
			bucketMilli = (ts.Samples[0].Timestamp / hourMs) * hourMs
		} else {
			bucketMilli = (time.Now().UnixMilli() / hourMs) * hourMs
		}

		// Dedup key must include metric_name: different metrics with
		// identical labels share the same fingerprint but need separate
		// rows in time_series.
		dedup := fmt.Sprintf("%d:%s", fp, metricName)
		if !seenSeries[dedup] {
			seenSeries[dedup] = true
			seriesRows = append(seriesRows, seriesRow{
				fingerprint: fp,
				metricName:  metricName,
				labels:      labels,
				unixMilli:   bucketMilli,
			})
		}

		for _, s := range ts.Samples {
			sampleRows = append(sampleRows, sampleRow{
				fingerprint: fp,
				metricName:  metricName,
				unixMilli:   s.Timestamp,
				value:       s.Value,
			})
		}
	}

	// INSERT time_series
	if len(seriesRows) > 0 {
		if err := w.insertTimeSeries(ctx, seriesRows); err != nil {
			slog.Error("write: insert time_series failed", "error", err, "rows", len(seriesRows))
		}
	}

	// INSERT samples
	if len(sampleRows) > 0 {
		if err := w.insertSamples(ctx, sampleRows); err != nil {
			slog.Error("write: insert samples failed", "error", err, "rows", len(sampleRows))
		}
	}

	slog.Debug("write: flush",
		"series", len(seriesRows),
		"samples", len(sampleRows),
		"duration", time.Since(t0),
	)
}

func labelsToJSON(m map[string]string) string {
	b, err := json.Marshal(m)
	if err != nil {
		slog.Error("labelsToJSON: marshal failed", "error", err)
		return "{}"
	}
	return string(b)
}

func (w *Writer) insertTimeSeries(ctx context.Context, rows []seriesRow) error {
	var colMN proto.ColStr
	var colFP proto.ColUInt64
	var colTS proto.ColInt64
	var colLabels proto.ColStr

	for _, r := range rows {
		colMN.Append(r.metricName)
		colFP.Append(r.fingerprint)
		colTS.Append(r.unixMilli)
		colLabels.Append(labelsToJSON(r.labels))
	}

	return w.pool.pool.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s.time_series VALUES", w.cfg.Database),
		Input: proto.Input{
			{Name: "metric_name", Data: &colMN},
			{Name: "fingerprint", Data: &colFP},
			{Name: "unix_milli", Data: &colTS},
			{Name: "labels", Data: &colLabels},
		},
	})
}

func (w *Writer) insertSamples(ctx context.Context, rows []sampleRow) error {
	var colFP proto.ColUInt64
	var colMN proto.ColStr
	var colTS proto.ColInt64
	var colVal proto.ColFloat64

	for _, r := range rows {
		colFP.Append(r.fingerprint)
		colMN.Append(r.metricName)
		colTS.Append(r.unixMilli)
		colVal.Append(r.value)
	}

	return w.pool.pool.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s.samples VALUES", w.cfg.Database),
		Input: proto.Input{
			{Name: "fingerprint", Data: &colFP},
			{Name: "metric_name", Data: &colMN},
			{Name: "unix_milli", Data: &colTS},
			{Name: "value", Data: &colVal},
		},
	})
}
