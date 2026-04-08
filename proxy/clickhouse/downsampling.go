package clickhouse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hinskii/promclick/proxy/config"
)

// escapeSQL escapes single quotes for ClickHouse SQL string literals.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

const createChecksumTableSQL = `
CREATE TABLE IF NOT EXISTS metrics.proxy_mv_checksums (
    mv_name    String,
    checksum   String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY mv_name`

// ApplyDownsamplingConfig creates tier tables, TTLs, MVs and backfills at startup.
func (p *Pool) ApplyDownsamplingConfig(ctx context.Context, cfg *config.DownsamplingConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("downsampling config invalid: %w", err)
	}

	// 0. Checksum table
	if err := p.Exec(ctx, createChecksumTableSQL); err != nil {
		return fmt.Errorf("ensure checksum table: %w", err)
	}

	// 1. Create tier tables
	for _, tier := range cfg.Tiers {
		slog.Info("ensuring tier table", "table", tier.Table)
		if err := p.createTierTableIfNotExists(ctx, tier); err != nil {
			return fmt.Errorf("create table %s: %w", tier.Table, err)
		}
	}

	// 2. Apply TTLs
	slog.Info("applying TTL", "table", "samples", "retention", cfg.RawRetention)
	if err := p.applyTTL(ctx, "samples",
		"toDateTime(fromUnixTimestamp64Milli(unix_milli))",
		cfg.RawRetention.Duration); err != nil {
		return fmt.Errorf("raw TTL: %w", err)
	}
	for _, tier := range cfg.Tiers {
		slog.Info("applying TTL", "table", tier.Table, "retention", tier.Retention)
		if err := p.applyTTL(ctx, tier.Table, "ts", tier.Retention.Duration); err != nil {
			return fmt.Errorf("TTL %s: %w", tier.Name, err)
		}
	}

	// 3. MVs (only recreate when config changed)
	for i, tier := range cfg.Tiers {
		sourceTable := "samples"
		if i > 0 {
			sourceTable = cfg.Tiers[i-1].Table
		}
		if err := p.createOrReplaceMVIfChanged(ctx, tier, sourceTable); err != nil {
			return fmt.Errorf("MV %s: %w", tier.Name, err)
		}
	}

	// 4. Backfill if empty
	for i, tier := range cfg.Tiers {
		sourceTable := "samples"
		if i > 0 {
			sourceTable = cfg.Tiers[i-1].Table
		}
		if err := p.backfillTierIfEmpty(ctx, tier, sourceTable); err != nil {
			return fmt.Errorf("backfill %s: %w", tier.Name, err)
		}
	}

	slog.Info("downsampling configuration applied")
	return nil
}

func (p *Pool) createTierTableIfNotExists(ctx context.Context, tier config.TierConfig) error {
	days := int(tier.Retention.Duration.Hours() / 24)
	sql := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS metrics.%s (
    metric_name   LowCardinality(String),
    fingerprint   UInt64                                  CODEC(Delta(8), ZSTD(1)),
    ts            DateTime                                CODEC(DoubleDelta, ZSTD(1)),

    val_min       SimpleAggregateFunction(min, Float64)  CODEC(Gorilla, ZSTD(1)),
    val_max       SimpleAggregateFunction(max, Float64)  CODEC(Gorilla, ZSTD(1)),
    val_sum       SimpleAggregateFunction(sum, Float64)  CODEC(Gorilla, ZSTD(1)),
    val_count     SimpleAggregateFunction(sum, UInt64)   CODEC(T64, ZSTD(1)),
    counter_total SimpleAggregateFunction(sum, Float64)  CODEC(Gorilla, ZSTD(1)),
    first_time    SimpleAggregateFunction(min, Int64)    CODEC(DoubleDelta, ZSTD(1)),
    last_time     SimpleAggregateFunction(max, Int64)    CODEC(DoubleDelta, ZSTD(1)),

    first_value   AggregateFunction(argMin, Float64, Int64),
    last_value    AggregateFunction(argMax, Float64, Int64)

) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (metric_name, fingerprint, ts)
TTL ts + INTERVAL %d DAY DELETE
SETTINGS ttl_only_drop_parts = 1`,
		tier.Table, days)

	return p.Exec(ctx, sql)
}

func (p *Pool) applyTTL(ctx context.Context, table, tsExpr string, d time.Duration) error {
	days := int(d.Hours() / 24)
	return p.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE metrics.%s MODIFY TTL %s + INTERVAL %d DAY DELETE`,
		table, tsExpr, days,
	))
}

func mvConfigChecksum(tier config.TierConfig, sourceTable string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s|%s",
		tier.CompactAfter.String(),
		tier.Resolution.String(),
		tier.Table,
		sourceTable,
	)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func (p *Pool) createOrReplaceMVIfChanged(ctx context.Context, tier config.TierConfig, sourceTable string) error {
	mvName := fmt.Sprintf("mv_downsample_%s", tier.Name)
	newChecksum := mvConfigChecksum(tier, sourceTable)

	// Check existing checksum
	var existingChecksum string
	err := p.QueryRow(ctx,
		fmt.Sprintf("SELECT checksum FROM metrics.proxy_mv_checksums FINAL WHERE mv_name = '%s' LIMIT 1", escapeSQL(mvName)),
		&existingChecksum,
	)

	if err == nil && existingChecksum == newChecksum {
		slog.Info("MV unchanged, skipping", "mv", mvName, "checksum", newChecksum[:8])
		return nil
	}

	if err == nil && existingChecksum != "" {
		slog.Info("MV config changed, recreating", "mv", mvName)
	} else {
		slog.Info("creating MV", "mv", mvName)
	}

	// Drop existing
	_ = p.Exec(ctx, fmt.Sprintf("DROP VIEW IF EXISTS metrics.%s", mvName))

	// Create new MV
	var createSQL string
	if sourceTable == "samples" {
		createSQL = buildRawToTierMVSQL(mvName, tier)
	} else {
		createSQL = buildTierToTierMVSQL(mvName, tier, sourceTable)
	}

	if err := p.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("create MV %s: %w", mvName, err)
	}

	// Save checksum
	return p.Exec(ctx, fmt.Sprintf(
		"INSERT INTO metrics.proxy_mv_checksums (mv_name, checksum) VALUES ('%s', '%s')",
		escapeSQL(mvName), escapeSQL(newChecksum),
	))
}

func (p *Pool) backfillTierIfEmpty(ctx context.Context, tier config.TierConfig, sourceTable string) error {
	type countRow struct {
		C json.Number `json:"c"`
	}
	var cr countRow
	if err := p.queryJSON(ctx,
		fmt.Sprintf("SELECT count() AS c FROM metrics.%s", tier.Table),
		&cr,
	); err != nil {
		return err
	}
	count, _ := cr.C.Int64()
	if count > 0 {
		slog.Info("tier has data, skip backfill", "tier", tier.Name, "rows", count)
		return nil
	}

	if sourceTable == "samples" {
		return p.backfillRawInChunks(ctx, tier)
	}

	slog.Info("backfilling tier", "tier", tier.Table, "source", sourceTable)
	sql := buildTierBackfillSQL(tier, sourceTable)
	if err := p.Exec(ctx, sql); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	slog.Info("backfill done", "tier", tier.Table)
	return nil
}

// backfillRawInChunks processes raw→tier backfill one day at a time to avoid OOM.
func (p *Pool) backfillRawInChunks(ctx context.Context, tier config.TierConfig) error {
	compactAfterHours := int(tier.CompactAfter.Duration.Hours())
	bucketExpr := startOfIntervalExpr(
		"toDateTime(intDiv(unix_milli, 1000))",
		tier.Resolution.Duration,
	)

	// Find data range via HTTP
	type minMax struct {
		Min json.Number `json:"min_ms"`
		Max json.Number `json:"max_ms"`
	}
	var mm minMax
	if err := p.queryJSON(ctx, "SELECT min(unix_milli) AS min_ms, max(unix_milli) AS max_ms FROM metrics.samples", &mm); err != nil {
		return fmt.Errorf("query data range: %w", err)
	}
	minMs, _ := mm.Min.Int64()
	maxMs, _ := mm.Max.Int64()
	if minMs == 0 || maxMs == 0 {
		slog.Info("no samples to backfill")
		return nil
	}

	endMs := time.Now().Add(-time.Duration(compactAfterHours) * time.Hour).UnixMilli()
	if maxMs < endMs {
		endMs = maxMs
	}

	dayMs := int64(1 * 60 * 60 * 1000) // 1h chunks to avoid OOM with window functions on large datasets
	chunkStart := minMs
	day := 1

	for chunkStart < endMs {
		chunkEnd := chunkStart + dayMs
		if chunkEnd > endMs {
			chunkEnd = endMs
		}

		sql := fmt.Sprintf(`
INSERT INTO metrics.%s
SELECT
    metric_name, fingerprint,
    %s AS ts,
    min(value)                     AS val_min,
    max(value)                     AS val_max,
    sum(value)                     AS val_sum,
    toUInt64(count())              AS val_count,
    greatest(0, argMax(value, unix_milli) - argMin(value, unix_milli)) AS counter_total,
    min(unix_milli)                AS first_time,
    max(unix_milli)                AS last_time,
    argMinState(value, unix_milli) AS first_value,
    argMaxState(value, unix_milli) AS last_value
FROM metrics.samples
WHERE unix_milli >= %d AND unix_milli < %d
GROUP BY metric_name, fingerprint, ts
SETTINGS max_memory_usage = 4000000000`,
			tier.Table, bucketExpr, chunkStart, chunkEnd)

		if err := p.Exec(ctx, sql); err != nil {
			return fmt.Errorf("backfill chunk day %d: %w", day, err)
		}
		slog.Info("backfill chunk done", "tier", tier.Table, "day", day)
		chunkStart = chunkEnd
		day++
	}

	slog.Info("backfill complete", "tier", tier.Table, "days", day-1)
	return nil
}

// startOfIntervalExpr returns a SQL expression for bucketing a DateTime column.
func startOfIntervalExpr(col string, d time.Duration) string {
	switch d {
	case time.Minute:
		return fmt.Sprintf("toStartOfMinute(%s)", col)
	case 5 * time.Minute:
		return fmt.Sprintf("toStartOfFiveMinutes(%s)", col)
	case 10 * time.Minute:
		return fmt.Sprintf("toStartOfTenMinutes(%s)", col)
	case 15 * time.Minute:
		return fmt.Sprintf("toStartOfFifteenMinutes(%s)", col)
	case time.Hour:
		return fmt.Sprintf("toStartOfHour(%s)", col)
	case 24 * time.Hour:
		return fmt.Sprintf("toStartOfDay(%s)", col)
	default:
		secs := int(d.Seconds())
		return fmt.Sprintf("toStartOfInterval(%s, INTERVAL %d SECOND)", col, secs)
	}
}

// chInterval formats a duration as a ClickHouse INTERVAL string (e.g. "5 MINUTE", "1 HOUR").
func chInterval(d time.Duration) string {
	if d >= 24*time.Hour && d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%d DAY", int(d.Hours()/24))
	}
	if d >= time.Hour && d%time.Hour == 0 {
		return fmt.Sprintf("%d HOUR", int(d.Hours()))
	}
	if d >= time.Minute && d%time.Minute == 0 {
		return fmt.Sprintf("%d MINUTE", int(d.Minutes()))
	}
	return fmt.Sprintf("%d SECOND", int(d.Seconds()))
}

// buildRawToTierMVSQL — REFRESH MV: raw samples → first tier (e.g. 5m)
func buildRawToTierMVSQL(mvName string, tier config.TierConfig) string {
	compactAfterHours := int(tier.CompactAfter.Duration.Hours())
	overlapHours := compactAfterHours + 1
	bucketExpr := startOfIntervalExpr(
		"toDateTime(intDiv(unix_milli, 1000))",
		tier.Resolution.Duration,
	)

	return fmt.Sprintf(`
CREATE MATERIALIZED VIEW metrics.%s
REFRESH EVERY %s
TO metrics.%s
AS
WITH samples_with_delta AS (
    SELECT
        metric_name, fingerprint, unix_milli, value,
        %s AS bucket,
        lagInFrame(value, 1, 0) OVER (
            PARTITION BY fingerprint
            ORDER BY unix_milli ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS prev_value,
        row_number() OVER (
            PARTITION BY fingerprint
            ORDER BY unix_milli ASC
        ) AS rn
    FROM metrics.samples
    WHERE unix_milli >= toInt64(toUnixTimestamp(now() - INTERVAL %d HOUR - INTERVAL 1 MINUTE)) * 1000
      AND unix_milli <  toInt64(toUnixTimestamp(now() - INTERVAL %d HOUR)) * 1000
)
SELECT
    metric_name,
    fingerprint,
    bucket                         AS ts,
    min(value)                     AS val_min,
    max(value)                     AS val_max,
    sum(value)                     AS val_sum,
    toUInt64(count())              AS val_count,
    sum(
        CASE
            WHEN rn = 1              THEN 0
            WHEN value >= prev_value THEN value - prev_value
            ELSE value
        END
    )                              AS counter_total,
    min(unix_milli)                AS first_time,
    max(unix_milli)                AS last_time,
    argMinState(value, unix_milli) AS first_value,
    argMaxState(value, unix_milli) AS last_value
FROM samples_with_delta
WHERE unix_milli >= toInt64(toUnixTimestamp(now() - INTERVAL %d HOUR)) * 1000
GROUP BY metric_name, fingerprint, bucket`,
		mvName, chInterval(tier.Resolution.Duration), tier.Table,
		bucketExpr,
		overlapHours, compactAfterHours,
		compactAfterHours,
	)
}

// buildTierToTierMVSQL — REFRESH MV: tier → coarser tier (e.g. 5m → 1h)
func buildTierToTierMVSQL(mvName string, tier config.TierConfig, sourceTable string) string {
	compactAfterHours := int(tier.CompactAfter.Duration.Hours())
	overlapHours := compactAfterHours + 2
	bucketExpr := startOfIntervalExpr("ts", tier.Resolution.Duration)
	windowStart := startOfIntervalExpr(
		fmt.Sprintf("now() - INTERVAL %d HOUR - INTERVAL 2 HOUR", overlapHours),
		tier.Resolution.Duration,
	)
	windowEnd := startOfIntervalExpr(
		fmt.Sprintf("now() - INTERVAL %d HOUR", compactAfterHours),
		tier.Resolution.Duration,
	)

	return fmt.Sprintf(`
CREATE MATERIALIZED VIEW metrics.%s
REFRESH EVERY %s
TO metrics.%s
AS
SELECT
    metric_name,
    fingerprint,
    bucket                          AS ts,
    min(val_min)                    AS val_min,
    max(val_max)                    AS val_max,
    sum(val_sum)                    AS val_sum,
    sum(val_count)                  AS val_count,
    sum(counter_total)              AS counter_total,
    min(first_time)                 AS first_time,
    max(last_time)                  AS last_time,
    argMinMergeState(first_value)   AS first_value,
    argMaxMergeState(last_value)    AS last_value
FROM (
    SELECT *, %s AS bucket
    FROM metrics.%s
    WHERE ts >= %s
      AND ts <  %s
)
GROUP BY metric_name, fingerprint, bucket`,
		mvName, chInterval(tier.Resolution.Duration), tier.Table,
		bucketExpr,
		sourceTable,
		windowStart, windowEnd,
	)
}


// buildTierBackfillSQL — one-time backfill: tier → coarser tier
func buildTierBackfillSQL(tier config.TierConfig, sourceTable string) string {
	bucketExpr := startOfIntervalExpr("ts", tier.Resolution.Duration)
	compactAfterHours := int(tier.CompactAfter.Duration.Hours())
	boundaryExpr := startOfIntervalExpr(
		fmt.Sprintf("now() - INTERVAL %d HOUR", compactAfterHours),
		tier.Resolution.Duration,
	)

	return fmt.Sprintf(`
INSERT INTO metrics.%s
SELECT
    metric_name,
    fingerprint,
    bucket                          AS ts,
    min(val_min)                    AS val_min,
    max(val_max)                    AS val_max,
    sum(val_sum)                    AS val_sum,
    sum(val_count)                  AS val_count,
    sum(counter_total)              AS counter_total,
    min(first_time)                 AS first_time,
    max(last_time)                  AS last_time,
    argMinMergeState(first_value)   AS first_value,
    argMaxMergeState(last_value)    AS last_value
FROM (
    SELECT *, %s AS bucket
    FROM metrics.%s
    WHERE ts < %s
)
GROUP BY metric_name, fingerprint, bucket
SETTINGS max_memory_usage = 8000000000`,
		tier.Table,
		bucketExpr,
		sourceTable,
		boundaryExpr,
	)
}
