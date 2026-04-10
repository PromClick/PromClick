# PromClick

**I was tired of Thanos. I was tired of Victoria Metrics. I was tired of Grafana Mimir and I'm tired of everything.**

You know the drill. You want long-term Prometheus storage. So you deploy Thanos - suddenly you have 47 YAML files, a sidecar, a store gateway, a compactor, a query frontend, and a PhD in distributed systems. Or you go with Mimir - congrats, you now operate a distributed hash ring and pray to the Memberlist gods every Tuesday.

"Ok I found Clickhouse! But now I'll just use the ClickHouse datasource plugin for Grafana." Sure. Here's what happens when you try to express `rate(http_requests_total{job="api"}[5m])` in raw ClickHouse SQL:

```sql
SELECT t, fingerprint,
    (argMax(value, unix_milli) - argMin(value, unix_milli))
    / (max(unix_milli) - min(unix_milli)) * 1000
FROM samples
WHERE metric_name = 'http_requests_total'
  AND JSONExtractString(labels, 'job') = 'api'
GROUP BY fingerprint, toStartOfInterval(...) AS t
-- doesn't handle counter resets. or staleness. or extrapolation.
-- good luck with histogram_quantile. I'll wait.
```

Now copy-paste that into 200 dashboards. Tweak each one. Debug why the numbers don't match Prometheus. Realize half your alerts are wrong because you forgot counter reset handling in dashboard #47. Spend three sprints on it. Question your career choices.

So I started simple - just a CLI that translates PromQL to SQL. You know, a weekend project. `rate(x[5m])` in, SQL out, done. Then I thought "what if Grafana could talk to it directly?" So I added an HTTP layer. Then I needed staleness handling. Then counter resets. Then histogram_quantile. Then downsampling. Then a label cache. Then native TCP. Then...

**That weekend project is now PromClick.** A Prometheus-compatible HTTP API that translates PromQL to ClickHouse SQL in real-time. Drop it in front of Grafana, point your dashboards at it, and forget that Thanos ever existed.

```
Grafana  -->  PromClick (:9090)  -->  ClickHouse (Native TCP :9000)
                                 -->  ClickHouse (HTTP :8123 for DDL)
```

No sidecars. No hash rings. No store gateways. One binary. One ClickHouse table. Done.

---

## What it does

PromClick is a **PromQL-to-SQL transpiler + HTTP proxy** that makes ClickHouse speak Prometheus.

- **Full Prometheus API compatibility** - `/api/v1/query`, `/api/v1/query_range`, `/api/v1/labels`, `/api/v1/series`, `/api/v1/metadata`
- **70 PromQL functions** - rate, irate, increase, histogram_quantile, predict_linear, all *_over_time, all aggregations, all math functions
- **Thanos-style downsampling** - automatic 5m and 1h tiers with MV-based compaction
- **Prometheus remote_write ingestion** - receives metrics directly from Prometheus
- **100% value accuracy on raw queries** - verified per-series, per-timestamp against Prometheus on 10K series

### Benchmark: PromClick vs Prometheus

Tested on 10,000 series (counter + gauge + histogram) over 30 days:

| Query | PromClick | Prometheus | Speedup | Accuracy |
|-------|-----------|------------|---------|----------|
| `rate(counter[5m])` 1111 series | **100ms** | 170ms | **1.7x faster** | 100% |
| `sum by(region)(rate(counter[5m]))` | **245ms** | 250ms | ~parity | 100% |
| `avg_over_time(gauge[5m])` 1111 series | **72ms** | 115ms | **1.6x faster** | 100% |
| `histogram_quantile(0.99, rate(bucket[5m]))` | **290ms** | 111ms | 2.6x slower | 100% |
| `count by(region)(metric)` | **1ms** | 69ms | **62x faster** | 100% |
| `rate(counter[2h])` 30d, 1h step | **2.3s** | 13.3s | **5.5x faster** | 100% |
| `avg_over_time(gauge[2h])` 30d | **0.9s** | 26.5s | **30x faster** | 100% |
| `sum(avg by(region)(rate))` 30d | **7.5s** | 41s | **5.5x faster** | 100% |

PromClick is **faster than Prometheus** for most query types. On long-range queries (7-30 days) with downsampling, it's **5-30x faster** because ClickHouse reads pre-aggregated data instead of scanning billions of raw samples.

Full benchmark report (38 queries, all tiers):

![Benchmark Report](img/benchmark_report.png)

---

## UI

PromClick ships with a built-in web UI for ad-hoc PromQL queries - syntax highlighting, autocomplete, graph + table views.

![PromClick Query UI](img/ui.png)

TSDB Status page shows series count, sample count, top metrics, and label cardinality - all served from ClickHouse:

![PromClick TSDB Status](img/ui2.png)

---

## Three Binaries, One Image

PromClick ships as a single Docker image with three binaries inside. Each handles one concern:

```bash
docker pull quay.io/hinski/promclick:0.1.0

docker run quay.io/hinski/promclick promclick-proxy       --config proxy.yaml
docker run quay.io/hinski/promclick promclick-writer      --config writer.yaml
docker run quay.io/hinski/promclick promclick-downsampler  --config downsampler.yaml
```

### promclick-proxy (query server)

The main binary. Serves the Prometheus HTTP API, translates PromQL to SQL, fetches from ClickHouse.

```yaml
# proxy.yaml
listen_addr: ":9099"
query_timeout: "2m"

clickhouse:
  native_addr: "clickhouse:9000"     # ch-go Native TCP (fast path)
  http_addr: "http://clickhouse:8123" # HTTP for DDL/metadata
  database: "metrics"

labels:
  cache_enabled: true    # in-memory label cache (eliminates JOIN)
  cache_ttl: "60s"       # refresh interval
  cache_max_series: 50000

downsampling:            # read-only - proxy uses tiers for query routing
  enabled: true          # but does NOT create tables/MVs
  tiers:
    - name: "5m"
      table: "samples_5m"
      compact_after: "40h"   # data older than 40h → read from 5m tier
      min_step: "60s"        # use tier when query step >= 60s
    - name: "1h"
      table: "samples_1h"
      compact_after: "240h"
      min_step: "3600s"
```

**What it does:** receives PromQL from Grafana, transpiles to SQL, fetches from ClickHouse (raw or downsampled tier based on step), evaluates in Go, returns Prometheus-compatible JSON.

**What it doesn't do:** write data, create tables, manage MVs.

### promclick-writer (remote_write receiver)

Receives Prometheus `remote_write` and batch-inserts into ClickHouse.

```yaml
# writer.yaml
listen_addr: ":9091"

clickhouse:
  native_addr: "clickhouse:9000"
  database: "metrics"

write:
  batch_size: 10000       # flush after N samples
  queue_size: 100000      # in-memory queue depth
  flush_interval: "5s"    # flush after 5s even if batch not full
```

**What it does:** `POST /api/v1/write` → snappy decompress → protobuf decode → batch INSERT into `samples` + `time_series` tables via Native TCP.

**What it doesn't do:** query data, create tier tables.

### promclick-downsampler (DDL + backfill)

One-shot binary that creates downsampling tier tables, Materialized Views, TTLs, and backfills historical data.

```yaml
# downsampler.yaml
clickhouse:
  native_addr: "clickhouse:9000"
  http_addr: "http://clickhouse:8123"
  database: "metrics"

downsampling:
  enabled: true
  raw_retention: "7d"           # TTL: delete raw samples after 7 days
  tiers:
    - name: "5m"
      resolution: "5m"          # bucket size
      table: "samples_5m"
      compact_after: "40h"
      retention: "90d"           # keep 5m data for 90 days
    - name: "1h"
      resolution: "1h"
      table: "samples_1h"
      compact_after: "240h"
      retention: "730d"          # keep 1h data for 2 years

daemon: false    # true = run in loop, false = one-shot and exit
interval: "1h"   # re-check interval in daemon mode
```

**What it does:**
1. `CREATE TABLE samples_5m` / `samples_1h` (AggregatingMergeTree)
2. `ALTER TABLE ... MODIFY TTL` on raw + tier tables
3. `CREATE MATERIALIZED VIEW ... REFRESH EVERY 5m/1h` - ClickHouse auto-aggregates
4. Backfill historical data (chunked, memory-limited)
5. Checksum-based: only recreates MVs when config changes

**Run as:** Kubernetes init container, CronJob, or `docker run --rm`.

### Docker Compose (all-in-one)

```bash
git clone https://github.com/PromClick/PromClick
cd promclick
docker compose up -d

# Wait ~60s, then open:
#   http://localhost:3000  - Grafana (admin/admin) with Node Exporter dashboard
#   http://localhost:9099  - PromClick UI
#   http://localhost:9090  - Prometheus
```

The compose stack includes: ClickHouse, Prometheus, Node Exporter, PromClick (proxy + writer + downsampler), and Grafana with pre-provisioned datasources and a Node Exporter dashboard.

---

## Architecture

```
promclick/                 -- root Go module (CLI + core)
|-- translator/            -- PromQL AST -> SQL transpilation
|-- eval/                  -- Go-side PromQL evaluation engine
|-- clickhouse/            -- HTTP client for ClickHouse
|-- types/                 -- Sample, Series, Vector, Matrix
|-- config/                -- YAML config + schema detection
|-- fingerprint/           -- xxhash64 series fingerprinting

proxy/                     -- HTTP proxy module (3 binaries)
|-- cmd/proxy/             -- query server (read-only)
|-- cmd/writer/            -- remote_write receiver (write-only)
|-- cmd/downsampler/       -- DDL + backfill + MV management
|-- clickhouse/            -- ch-go Native TCP pool, label cache, tier queries
|-- server/                -- HTTP routes, handlers, middleware
|-- config/                -- per-binary YAML configs
|-- ui/                    -- React frontend (syntax highlighting, uPlot charts)
```

### Three binaries, one purpose

| Binary | Role | Scaling |
|--------|------|---------|
| `promclick-proxy` | Serves PromQL queries | Horizontal (N instances behind LB) |
| `promclick-writer` | Receives `remote_write` from Prometheus | 1-2 instances |
| `promclick-downsampler` | Creates tier tables, MVs, backfill | 1 instance (cron/init) |

---

## How PromQL becomes SQL

This is the core magic. PromClick doesn't approximate - it implements the exact Prometheus evaluation semantics in Go, using ClickHouse as the storage backend.

### Step 1: Parse & Transpile

```
rate(http_requests_total{job="api"}[5m])
```

The Prometheus parser produces an AST. PromClick's transpiler walks it and produces a `SQLPlan` - an intermediate representation that captures:

- **Metric name** and **label matchers** (for SQL WHERE)
- **Function name** (rate, irate, increase...) - evaluated in Go
- **Range window** (5m → data fetch window with staleness buffer)
- **Aggregation chain** (`sum by(x)(rate(...))` → `[rate, sum_by_x]`)
- **Math chain** (`abs(ceil(rate(...)))` → applied post-aggregation)
- **Binary ops** (`A / B` → parallel LHS/RHS evaluation)
- **Offset, @modifier** (time shift for data fetch)

### Step 2: Fetch from ClickHouse

The SQL fetches raw samples:

```sql
SELECT fingerprint, unix_milli AS ts, value
FROM samples
PREWHERE metric_name = 'http_requests_total'
WHERE unix_milli > {start} AND unix_milli <= {end}
  AND fingerprint IN (12345, 67890, ...)   -- from label cache
ORDER BY fingerprint, unix_milli
```

Key optimizations:
- **Label cache** - in-memory `fingerprint -> labels` map, refreshed every 60s. Eliminates JOIN to `time_series` table. Fingerprints resolved in Go via regex matching.
- **Native TCP** - ch-go protocol, 2x faster than HTTP+JSON
- **UInt64 fingerprints** - zero string allocations in hot path
- **Parallel fetch** - splits fingerprints into chunks for concurrent CH queries
- **PREWHERE** - ClickHouse reads metric_name column first, skips irrelevant granules

### Step 3: Evaluate in Go

PromClick implements every PromQL function natively:

**Rate/Increase/Delta** - Prometheus-exact extrapolation:
```go
// Counter reset correction
for _, s := range samples {
    if isCounter && s.Value < lastVal {
        counterCorrection += lastVal  // reset detected
    }
}
resultValue := last.Value - first.Value + counterCorrection

// Edge extrapolation (identical to Prometheus)
durationToStart := float64(first.Timestamp - rangeStartMs) / 1000.0
extrapolateToInterval := sampledInterval
if durationToStart < avgInterval * 1.1 {
    extrapolateToInterval += durationToStart
} else {
    extrapolateToInterval += avgInterval / 2
}
resultValue *= extrapolateToInterval / sampledInterval
```

**WindowSamples** - binary search, zero allocations:
```go
func WindowSamples(samples []Sample, rangeStart, rangeEnd int64) []Sample {
    lo := sort.Search(len(samples), func(i int) bool {
        return samples[i].Timestamp > rangeStart  // left-open
    })
    hi := sort.Search(len(samples), func(i int) bool {
        return samples[i].Timestamp > rangeEnd    // right-closed
    })
    return samples[lo:hi]  // zero-copy slice
}
```

**Staleness** - exact NaN detection:
```go
const StaleNaNBits uint64 = 0x7FF0000000000002
func IsStaleNaN(v float64) bool {
    return math.Float64bits(v) == StaleNaNBits
}
```

**Histogram quantile** - bucket interpolation with monotonicity enforcement, identical to Prometheus.

**Aggregations** - Kahan-Neumaier compensated summation (same numerical precision as Prometheus):
```go
func kahanSumInc(inc, sum, c float64) (float64, float64) {
    t := sum + inc
    if math.Abs(sum) >= math.Abs(inc) {
        c += (sum - t) + inc
    } else {
        c += (inc - t) + sum
    }
    return t, c
}
```

### Step 4: Series-first evaluation

For range queries, PromClick uses a **series-first** iteration pattern instead of Prometheus's step-first:

```
Prometheus:  for each step -> for each series -> compute
PromClick:   for each series -> for each step -> compute  (better cache locality)
```

This means:
- Fingerprint computed **once** per series (not per step)
- No map iteration per step
- Samples stay in L1 cache across steps
- Aggregation uses pre-computed group keys

### Step 5: Streaming JSON response

Large responses (1000+ series) are written directly to the HTTP writer using `bufio.Writer`:

```go
bw.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
for si, s := range matrix {
    // write directly, no intermediate []interface{} allocations
    bw.WriteString(`{"metric":`)
    lb, _ := json.Marshal(s.Labels)
    bw.Write(lb)
    bw.WriteString(`,"values":[`)
    for vi, p := range s.Samples {
        // format inline - zero alloc per datapoint
    }
}
```

This eliminates millions of `interface{}` allocations for large result sets.

---

## Downsampling

PromClick implements **Thanos-style downsampling** using ClickHouse Materialized Views with REFRESH.

### How it works

```
Raw samples (15s intervals)
    |
    v  [REFRESH MV every 5m]
samples_5m (5-minute aggregates: min, max, sum, count, counter_total)
    |
    v  [REFRESH MV every 1h]
samples_1h (1-hour aggregates)
```

Each tier stores per-bucket aggregates:

| Column | Type | Purpose |
|--------|------|---------|
| `val_min` | Float64 | Minimum value in bucket |
| `val_max` | Float64 | Maximum value in bucket |
| `val_sum` | Float64 | Sum of values |
| `val_count` | UInt64 | Number of samples |
| `counter_total` | Float64 | Sum of counter deltas |
| `first_time` / `last_time` | Int64 | Actual sample time span |
| `first_value` / `last_value` | argMin/argMax state | For extrapolation |

### Query routing

For each query, PromClick picks the best data source based on step size:

```yaml
downsampling:
  tiers:
    - name: "5m"
      min_step: "60s"       # use when step >= 60s
      compact_after: "40h"  # data older than 40h reads from tier
    - name: "1h"
      min_step: "3600s"     # use when step >= 1h
      compact_after: "240h"
```

Time range is split into segments via **UNION ALL**:

```
Query: last 30 days, step=1h
  samples_1h [30d ago, 10d ago)   -- oldest data from 1h tier
  samples_5m [10d ago, 40h ago)   -- mid-range from 5m tier
  samples    [40h ago, now)       -- recent data from raw
```

### Gauge functions on tiers

For `avg_over_time`, `min_over_time`, etc., PromClick pushes the computation to ClickHouse:

```sql
SELECT fingerprint,
    toInt64(toUnixTimestamp(toStartOfFiveMinutes(ts))) * 1000 AS step_ts,
    sum(val_sum) AS val_sum,
    sum(val_count) AS val_count,
    min(val_min) AS val_min,
    max(val_max) AS val_max
FROM samples_5m
GROUP BY fingerprint, step_ts
```

Go just picks the right column - no windowed eval needed. This is why `avg_over_time` on 30 days is **30x faster** than Prometheus.

### Counter functions on tiers

For `rate`/`increase`, PromClick uses a **sliding window** with two pointers (O(steps + buckets) per series):

```go
lo := 0
for _, evalTimeMs := range steps {
    // advance left pointer past expired buckets
    for lo < len(buckets) && buckets[lo].Timestamp <= windowStart {
        lo++
    }
    // scan window from lo
    for i := lo; i < len(buckets) && buckets[i].Timestamp <= windowEnd; i++ {
        sumDelta += buckets[i].CounterTotal
    }
    value = sumDelta * extrapolationFactor / rangeSec
}
```

---

## Data Schema

PromClick uses a flat schema (inspired by SigNoz):

```sql
-- Raw samples
CREATE TABLE samples (
    metric_name  LowCardinality(String),
    fingerprint  UInt64,
    unix_milli   Int64,
    value        Float64
) ENGINE = MergeTree()
ORDER BY (metric_name, fingerprint, unix_milli)

-- Series metadata (labels as JSON string)
CREATE TABLE time_series (
    metric_name  LowCardinality(String),
    fingerprint  UInt64,
    labels       String,  -- JSON: {"job":"api","instance":"host-1"}
    unix_milli   Int64    -- hour-bucketed for dedup
) ENGINE = MergeTree()
ORDER BY (metric_name, fingerprint, unix_milli)
```

**Why JSON labels instead of Map?** `JSONExtractString(labels, 'key')` is 10x faster than `labels['key']` on Map columns. Proven at scale by SigNoz.

**Why not ReplacingMergeTree?** Time-bucketing `unix_milli` to the hour gives natural deduplication without FINAL overhead.

---

## Label Cache

PromClick keeps an in-memory cache of all `fingerprint -> labels` mappings:

```
GetFingerprints("http_requests", [{job, =, api}])
  -> [12345, 67890, ...]  (filtered in Go with cached regex)
```

This eliminates the JOIN to `time_series` on every query. The cache refreshes every 60s via HTTP. For `count by(region)(metric)`, PromClick answers **entirely from cache** - zero ClickHouse queries, **1ms** response time.

---

## Performance Stack

| Optimization | Effect |
|---|---|
| ch-go Native TCP | 2x faster than HTTP+JSON |
| Label cache | Eliminates JOIN per query |
| UInt64 fingerprint in fetch | Zero string allocs (800K/query) |
| Parallel fetch (4 chunks) | Splits CH queries across pool |
| Series-first evaluation | Better cache locality, fingerprint computed once |
| Pre-computed group keys | Eliminates 800K MatchingKey calls in aggregation |
| Sliding window eval | O(steps + buckets) instead of O(steps * buckets) |
| Gauge SQL push-down | GROUP BY step in CH, no Go eval |
| Cache-only aggregation | count/group answered from RAM |
| Streaming JSON | Direct write to ResponseWriter, no intermediate allocs |
| Gzip with sync.Pool | Reusable compressors |
| Connection warmup | Zero cold-start penalty |
| Sorted IN clause | Better CH index utilization |
| slices.SortFunc | No interface boxing allocations |
| Kahan summation | Numerical precision matching Prometheus |

### Benchmark: eval micro-benchmarks

```
BenchmarkWindowSamples       24ns/op    0 allocs
BenchmarkExtrapolatedRate    37ns/op    0 allocs
BenchmarkInstantValue        12ns/op    0 allocs
BenchmarkHistogramQuantile  223ns/op    1 alloc
```

---

## Supported PromQL

### Functions (70)

**Rate family:** rate, irate, increase, delta, idelta, deriv, predict_linear, resets, changes

**Over-time:** avg_over_time, min_over_time, max_over_time, sum_over_time, count_over_time, stddev_over_time, stdvar_over_time, last_over_time, present_over_time, quantile_over_time, mad_over_time, double_exponential_smoothing

**Aggregations:** sum, avg, min, max, count, group, stddev, stdvar, topk, bottomk, quantile, count_values, limitk, limit_ratio

**Math:** abs, ceil, floor, round, sqrt, exp, ln, log2, log10, sgn, clamp, clamp_min, clamp_max

**Trig:** sin, cos, tan, asin, acos, atan, sinh, cosh, tanh, asinh, acosh, atanh, deg, rad, pi

**Label:** label_replace, label_join, sort, sort_desc, sort_by_label, sort_by_label_desc

**Other:** histogram_quantile, absent, absent_over_time, scalar, vector, time

### Binary operators

`+`, `-`, `*`, `/`, `%`, `^`, `==`, `!=`, `>`, `<`, `>=`, `<=`, `and`, `or`, `unless`, `atan2`

With full support for: `on()`, `ignoring()`, `group_left()`, `group_right()`, `bool`

### Not yet supported

- Subqueries `metric[1h:5m]`
- Native histograms
- `@ timestamp` modifier

---

## Quick Start

```bash
git clone https://github.com/PromClick/PromClick
cd promclick
docker compose up -d
```

Wait ~60s for data, then open:
- **http://localhost:3000** - Grafana (admin/admin) with Node Exporter dashboard
- **http://localhost:9099** - PromClick UI
- **http://localhost:9090** - Prometheus

Or run it yourself - check the [docker-compose.yaml](docker-compose.yaml) and [deploy/](deploy/) folder for all configs.

The compose stack runs everything: ClickHouse, Prometheus, Node Exporter, PromClick (proxy + writer + downsampler), and Grafana with two pre-provisioned datasources (PromClick + Prometheus) so you can compare side by side.

(You must wait few minutes untill Prometheus will start sending samples to PromClick)
---

## Project Stats

| Metric | Value |
|--------|-------|
| Go source files | 76 |
| Lines of Go | 14,491 |
| Unit tests | 130 |
| PromQL functions | 70 |
| Benchmark queries | 38 (100% accuracy vs Prometheus) |
| Spec documents | 30 |
| Binaries | 3 (proxy, writer, downsampler) |

---

## Why not just use [X]?

| Solution | Problem PromClick solves |
|----------|------------------------|
| **Thanos** | 6 components, S3 dependency, complex operations |
| **Mimir** | Distributed hash ring, Memberlist, complex scaling |
| **VictoriaMetrics** | Separate query language (MetricsQL), vendor lock-in |
| **M3DB** | Deprecated, complex, resource-hungry |
| **PromClick** | One binary, one ClickHouse table, full PromQL, done |

If you already run ClickHouse, PromClick gives you infinite Prometheus retention with zero additional infrastructure.

---

## TODO

- [ ] **Scraper** - scrape targets directly, drop Prometheus entirely
- [ ] **Ruler** - PromQL rule evaluation engine with Alertmanager integration
- [*] **Helm chart** - one-click deploy to Kubernetes
- [ ] **K8s operator** - CRD-based management of PromClick instances
- [ ] **Redis / Memcached query cache** - sub-millisecond responses for repeated queries
- [ ] Subquery support (`metric[1h:5m]`)
- [ ] Native histograms
- [ ] TLS
- [ ] Move clickhouse user and password from config files to environment variables
---

## License

Apache 2.0

---

*Built out of frustration with the Prometheus long-term storage ecosystem. If you've ever debugged a Thanos Compactor at 3am, wondered why Thanos Store eats 64GB of RAM to serve a single dashboard, or spent a sprint migrating from one TSDB to another just to end up with the same problems in a different color - this is for you.*

*P.S. If your monitoring stack has more components than the system it monitors, something went wrong along the way.*
