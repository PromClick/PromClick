CREATE DATABASE IF NOT EXISTS metrics;

CREATE TABLE IF NOT EXISTS metrics.samples (
    fingerprint UInt64              CODEC(Delta(8), ZSTD(1)),
    metric_name LowCardinality(String),
    unix_milli  Int64               CODEC(DoubleDelta, ZSTD(1)),
    value       Float64             CODEC(Gorilla, ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(unix_milli))
ORDER BY (metric_name, fingerprint, unix_milli);

CREATE TABLE IF NOT EXISTS metrics.time_series (
    metric_name  LowCardinality(String),
    fingerprint  UInt64             CODEC(Delta(8), ZSTD(1)),
    unix_milli   Int64              CODEC(Delta(8), ZSTD(1)),
    labels       String             CODEC(ZSTD(5))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(unix_milli))
ORDER BY (metric_name, fingerprint, unix_milli);

CREATE TABLE IF NOT EXISTS metrics.prom_metrics (
    metric_name LowCardinality(String),
    metric_type LowCardinality(String)
) ENGINE = ReplacingMergeTree
ORDER BY metric_name;
