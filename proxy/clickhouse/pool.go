package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"

	chclient "github.com/PromClick/PromClick/clickhouse"
	"github.com/PromClick/PromClick/types"
)

// SchemaInfo holds schema metadata for the flat schema.
type SchemaInfo struct {
	Database        string
	SamplesTable    string
	TimeSeriesTable string
	FingerprintCol  string
	TimestampCol    string
	ValueCol        string
	MetricNameCol   string
}

// Pool wraps a ch-go connection pool for native TCP queries.
type Pool struct {
	pool         *chpool.Pool
	LabelCache   *LabelCache
	Schema       SchemaInfo
	HTTPClient   *http.Client // shared HTTP client with connection limits
	httpAddr     string       // CH HTTP addr for DDL/INSERT (e.g. "localhost:18123")
	httpUser     string
	httpPassword string
}

// NewPool creates a native TCP connection pool to ClickHouse.
// httpAddr is used for DDL/INSERT...SELECT (e.g. "http://localhost:18123").
func NewPool(addr, database, user, password, httpAddr string, schema SchemaInfo) (*Pool, error) {
	opts := chpool.Options{
		ClientOptions: ch.Options{
			Address:          addr,
			Database:         database,
			User:             user,
			Password:         password,
			Compression:      ch.CompressionLZ4,
			DialTimeout:      5 * time.Second,
			HandshakeTimeout: 5 * time.Second,
		},
		MaxConnLifetime: 30 * time.Minute,
		MaxConnIdleTime: 5 * time.Minute,
	}
	p, err := chpool.New(context.Background(), opts)
	if err != nil {
		return nil, fmt.Errorf("chpool.New: %w", err)
	}

	pool := &Pool{
		pool:   p,
		Schema: schema,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        20,
				MaxIdleConnsPerHost: 10,
				MaxConnsPerHost:     20,
				IdleConnTimeout:     90 * time.Second,
			},
			Timeout: 5 * time.Minute,
		},
		httpAddr:     httpAddr,
		httpUser:     user,
		httpPassword: password,
	}

	slog.Info("schema detection",
		"table", schema.SamplesTable,
	)

	return pool, nil
}

// FetchSeriesData satisfies eval.DataFetcher.
// Uses label cache fast-path when available, falls back to JOIN SQL.
func (p *Pool) FetchSeriesData(ctx context.Context, sql string, params *chclient.QueryParams) (map[string]*chclient.SeriesData, error) {
	// Try cache fast-path: extract metric name + matchers from params,
	// query samples-only SQL (no JOIN), labels from cache
	if p.LabelCache != nil && p.LabelCache.IsLoaded() {
		metricName := extractParam(params, "metricName")
		dataStart := extractParam(params, "dataStart")
		dataEnd := extractParam(params, "dataEnd")

		if metricName != "" && dataStart != "" && dataEnd != "" {
			matchers := extractMatchers(params)
			fps, ok := p.LabelCache.GetFingerprints(metricName, matchers)
			if ok {
				return p.fetchWithCache(ctx, metricName, dataStart, dataEnd, fps)
			}
		}
	}

	// Fallback: execute original SQL with JOIN
	return p.fetchWithJoin(ctx, sql, params)
}

// fetchWithCache — fast path: samples-only query + labels from cache.
// Uses UInt64 fingerprint directly (no toString conversion, no string allocs per row).
// Parallel fetch: splits fingerprints into chunks for concurrent CH queries.
func (p *Pool) fetchWithCache(ctx context.Context,
	metricName, dataStart, dataEnd string, fps []string) (map[string]*chclient.SeriesData, error) {

	if len(fps) == 0 {
		return make(map[string]*chclient.SeriesData), nil
	}

	// For small series counts, single query is faster (no goroutine overhead)
	const parallelThreshold = 500
	const numWorkers = 4

	if len(fps) < parallelThreshold {
		return p.fetchChunk(ctx, metricName, dataStart, dataEnd, fps)
	}

	// Split fps into chunks and fetch in parallel
	chunkSize := (len(fps) + numWorkers - 1) / numWorkers
	type chunkResult struct {
		data map[string]*chclient.SeriesData
		err  error
	}
	results := make([]chunkResult, numWorkers)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		lo := w * chunkSize
		hi := lo + chunkSize
		if lo >= len(fps) {
			break
		}
		if hi > len(fps) {
			hi = len(fps)
		}
		wg.Add(1)
		go func(idx int, chunk []string) {
			defer wg.Done()
			data, err := p.fetchChunk(ctx, metricName, dataStart, dataEnd, chunk)
			results[idx] = chunkResult{data: data, err: err}
		}(w, fps[lo:hi])
	}
	wg.Wait()

	// Merge results
	merged := make(map[string]*chclient.SeriesData, len(fps))
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		for k, v := range r.data {
			merged[k] = v
		}
	}

	t0 := time.Now()
	_ = t0
	slog.Debug("ch_fetch",
		"path", "cached+parallel",
		"metric", metricName,
		"fps", len(fps),
		"series", len(merged),
	)
	return merged, nil
}

// fetchChunk fetches samples for a subset of fingerprints.
func (p *Pool) fetchChunk(ctx context.Context,
	metricName, dataStart, dataEnd string, fps []string) (map[string]*chclient.SeriesData, error) {

	if len(fps) == 0 {
		return make(map[string]*chclient.SeriesData), nil
	}

	s := p.Schema
	escapedMetric := strings.ReplaceAll(metricName, "'", "''")
	fpCol := s.FingerprintCol
	tsCol := s.TimestampCol
	valCol := s.ValueCol

	var b strings.Builder
	fmt.Fprintf(&b, "SELECT s.%s AS fingerprint, s.%s AS ts, s.%s AS value\n", fpCol, tsCol, valCol)
	fmt.Fprintf(&b, "FROM %s AS s\n", s.SamplesTable)
	fmt.Fprintf(&b, "PREWHERE s.%s = '%s'\n", s.MetricNameCol, escapedMetric)
	fmt.Fprintf(&b, "WHERE s.%s > %s\n", tsCol, dataStart)
	fmt.Fprintf(&b, "  AND s.%s <= %s\n", tsCol, dataEnd)

	if inClause := buildFingerprintIN(fps, fmt.Sprintf("s.%s", fpCol)); inClause != "" {
		fmt.Fprintf(&b, "  %s\n", inClause)
	}

	fmt.Fprintf(&b, "ORDER BY s.%s ASC, s.%s ASC", fpCol, tsCol)

	type seriesData struct {
		labels  map[string]string
		samples []types.Sample
	}
	intResult := make(map[uint64]*seriesData, len(fps))

	var (
		colFP  proto.ColUInt64
		colTS  proto.ColInt64
		colVal proto.ColFloat64
	)

	err := p.pool.Do(ctx, ch.Query{
		Body: b.String(),
		Result: proto.Results{
			{Name: "fingerprint", Data: &colFP},
			{Name: "ts", Data: &colTS},
			{Name: "value", Data: &colVal},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				fp := colFP.Row(i)

				sd, ok := intResult[fp]
				if !ok {
					fpStr := strconv.FormatUint(fp, 10)
					labels, hit := p.LabelCache.GetLabels(fpStr)
					if !hit {
						labels = map[string]string{}
					}
					sd = &seriesData{labels: labels}
					intResult[fp] = sd
				}
				sd.samples = append(sd.samples, types.Sample{Timestamp: colTS.Row(i), Value: colVal.Row(i)})
			}
			return nil
		},
	})

	// Convert uint64 map → string map (required by DataFetcher interface)
	result := make(map[string]*chclient.SeriesData, len(intResult))
	for fp, sd := range intResult {
		result[strconv.FormatUint(fp, 10)] = &chclient.SeriesData{Labels: sd.labels, Samples: sd.samples}
	}
	if err != nil {
		return nil, fmt.Errorf("native (cached): %w", err)
	}
	slog.Debug("ch_fetch",
		"path", "cached+prewhere",
		"metric", metricName,
		"fps", len(fps),
		"series", len(result),
	)
	return result, nil
}

// fetchWithJoin — fallback: original SQL with JOIN + labels from response.
func (p *Pool) fetchWithJoin(ctx context.Context, sql string, params *chclient.QueryParams) (map[string]*chclient.SeriesData, error) {
	t0 := time.Now()
	totalRows := 0
	resolvedSQL := inlineParams(sql, params)
	result := make(map[string]*chclient.SeriesData)

	var (
		colFP     proto.ColStr
		colTS     proto.ColInt64
		colVal    proto.ColFloat64
		colLabels proto.ColStr
	)

	err := p.pool.Do(ctx, ch.Query{
		Body: resolvedSQL,
		Result: proto.Results{
			{Name: "fingerprint", Data: &colFP},
			{Name: "ts", Data: &colTS},
			{Name: "value", Data: &colVal},
			{Name: "labels", Data: &colLabels},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			totalRows += block.Rows
			for i := 0; i < block.Rows; i++ {
				fp := colFP.Row(i)
				ts := colTS.Row(i)
				val := colVal.Row(i)

				sd, ok := result[fp]
				if !ok {
					var lblMap map[string]string
					if err := json.Unmarshal([]byte(colLabels.Row(i)), &lblMap); err != nil {
						lblMap = map[string]string{}
					}
					sd = &chclient.SeriesData{Labels: lblMap}
					result[fp] = sd
				}
				sd.Samples = append(sd.Samples, types.Sample{Timestamp: ts, Value: val})
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("native (join): %w", err)
	}
	slog.Debug("ch_fetch",
		"path", "join",
		"rows", totalRows,
		"series", len(result),
		"duration", time.Since(t0),
	)
	return result, nil
}

// ExecTierQuery executes a downsampled query and returns Matrix result.
// The query returns (fingerprint, step_ts, value) rows.
func (p *Pool) ExecTierQuery(ctx context.Context, sql string, fps []string) (types.Matrix, error) {
	t0 := time.Now()
	totalRows := 0

	// Map fingerprint string → series data
	seriesMap := make(map[string]*types.Series)

	var (
		colFP  proto.ColUInt64
		colTS  proto.ColDateTime
		colVal proto.ColFloat64
	)

	err := p.pool.Do(ctx, ch.Query{
		Body: sql,
		Result: proto.Results{
			{Name: "fingerprint", Data: &colFP},
			{Name: "step_ts", Data: &colTS},
			{Name: "value", Data: &colVal},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			totalRows += block.Rows
			for i := 0; i < block.Rows; i++ {
				fp := fmt.Sprintf("%d", colFP.Row(i))
				ts := colTS.Row(i).UnixMilli()
				val := colVal.Row(i)

				s, ok := seriesMap[fp]
				if !ok {
					labels := map[string]string{}
					if p.LabelCache != nil {
						if cached, hit := p.LabelCache.GetLabels(fp); hit {
							labels = cached
						}
					}
					s = &types.Series{Labels: labels}
					seriesMap[fp] = s
				}
				s.Samples = append(s.Samples, types.Sample{Timestamp: ts, Value: val})
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("tier query: %w", err)
	}

	// Build matrix from map
	matrix := make(types.Matrix, 0, len(seriesMap))
	for _, s := range seriesMap {
		matrix = append(matrix, *s)
	}

	slog.Debug("tier_query",
		"rows", totalRows,
		"series", len(matrix),
		"duration", time.Since(t0),
	)
	return matrix, nil
}

// CounterBucket holds per-bucket counter data with timing info for extrapolation.
type CounterBucket struct {
	Timestamp    int64   // bucket ts in ms
	CounterTotal float64 // delta per bucket
	FirstTime    int64   // ms — first sample timestamp in bucket
	LastTime     int64   // ms — last sample timestamp in bucket
}

// CounterSeries holds per-bucket counter data for one fingerprint.
type CounterSeries struct {
	Labels  map[string]string
	Buckets []CounterBucket
}

// ExecTierQueryRaw executes a counter tier query returning per-bucket data
// with first_time/last_time for Prometheus-compatible extrapolation.
func (p *Pool) ExecTierQueryRaw(ctx context.Context, sql string, fps []string) (map[string]*CounterSeries, error) {
	t0 := time.Now()
	totalRows := 0
	seriesMap := make(map[string]*CounterSeries)

	var (
		colFP  proto.ColUInt64
		colTS  proto.ColInt64
		colVal proto.ColFloat64
		colFT  proto.ColInt64
		colLT  proto.ColInt64
	)

	err := p.pool.Do(ctx, ch.Query{
		Body: sql,
		Result: proto.Results{
			{Name: "fingerprint", Data: &colFP},
			{Name: "step_ts", Data: &colTS},
			{Name: "value", Data: &colVal},
			{Name: "ft", Data: &colFT},
			{Name: "lt", Data: &colLT},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			totalRows += block.Rows
			for i := 0; i < block.Rows; i++ {
				fp := fmt.Sprintf("%d", colFP.Row(i))
				s, ok := seriesMap[fp]
				if !ok {
					labels := map[string]string{}
					if p.LabelCache != nil {
						if cached, hit := p.LabelCache.GetLabels(fp); hit {
							labels = cached
						}
					}
					s = &CounterSeries{Labels: labels}
					seriesMap[fp] = s
				}
				s.Buckets = append(s.Buckets, CounterBucket{
					Timestamp:    colTS.Row(i),
					CounterTotal: colVal.Row(i),
					FirstTime:    colFT.Row(i),
					LastTime:     colLT.Row(i),
				})
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("tier query raw: %w", err)
	}

	slog.Debug("tier_query_raw", "rows", totalRows, "series", len(seriesMap), "duration", time.Since(t0))
	return seriesMap, nil
}

// GaugeBucket holds per-bucket gauge aggregates.
type GaugeBucket struct {
	Timestamp int64
	ValSum    float64
	ValCount  uint64
	ValMin    float64
	ValMax    float64
}

// ExecGaugeQuery executes a gauge tier query returning per-bucket data with 4 value columns.
func (p *Pool) ExecGaugeQuery(ctx context.Context, sql string, fps []string) (map[string]*GaugeSeries, error) {
	t0 := time.Now()
	totalRows := 0
	seriesMap := make(map[string]*GaugeSeries)

	var (
		colFP       proto.ColUInt64
		colTS       proto.ColInt64
		colValSum   proto.ColFloat64
		colValCount proto.ColUInt64
		colValMin   proto.ColFloat64
		colValMax   proto.ColFloat64
	)

	err := p.pool.Do(ctx, ch.Query{
		Body: sql,
		Result: proto.Results{
			{Name: "fingerprint", Data: &colFP},
			{Name: "step_ts", Data: &colTS},
			{Name: "val_sum", Data: &colValSum},
			{Name: "val_count", Data: &colValCount},
			{Name: "val_min", Data: &colValMin},
			{Name: "val_max", Data: &colValMax},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			totalRows += block.Rows
			for i := 0; i < block.Rows; i++ {
				fp := fmt.Sprintf("%d", colFP.Row(i))
				s, ok := seriesMap[fp]
				if !ok {
					labels := map[string]string{}
					if p.LabelCache != nil {
						if cached, hit := p.LabelCache.GetLabels(fp); hit {
							labels = cached
						}
					}
					s = &GaugeSeries{Labels: labels}
					seriesMap[fp] = s
				}
				s.Buckets = append(s.Buckets, GaugeBucket{
					Timestamp: colTS.Row(i),
					ValSum:    colValSum.Row(i),
					ValCount:  colValCount.Row(i),
					ValMin:    colValMin.Row(i),
					ValMax:    colValMax.Row(i),
				})
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gauge tier query: %w", err)
	}

	slog.Debug("gauge_tier_query", "rows", totalRows, "series", len(seriesMap), "duration", time.Since(t0))
	return seriesMap, nil
}

// GaugeSeries holds per-bucket gauge data for one fingerprint.
type GaugeSeries struct {
	Labels  map[string]string
	Buckets []GaugeBucket
}

// Exec executes a raw SQL statement via HTTP (for DDL, INSERT...SELECT, ALTER etc.)
// ch-go native TCP doesn't reliably handle INSERT...SELECT, so we use HTTP.
func (p *Pool) Exec(ctx context.Context, sql string) error {
	u := strings.TrimRight(p.httpAddr, "/") + "/?database=" + p.Schema.Database
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))
	if err != nil {
		return err
	}
	if p.httpUser != "" {
		req.SetBasicAuth(p.httpUser, p.httpPassword)
	}
	resp, err := p.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// QueryRow executes a query and scans the first row into dest.
func (p *Pool) QueryRow(ctx context.Context, sql string, dest ...interface{}) error {
	// Simple single-value scan for uint64
	if len(dest) == 1 {
		if ptr, ok := dest[0].(*uint64); ok {
			var col proto.ColUInt64
			err := p.pool.Do(ctx, ch.Query{
				Body:   sql,
				Result: proto.Results{{Name: "count()", Data: &col}},
				OnResult: func(_ context.Context, block proto.Block) error {
					if block.Rows > 0 {
						*ptr = col.Row(0)
					}
					return nil
				},
			})
			return err
		}
		if ptr, ok := dest[0].(*string); ok {
			var col proto.ColStr
			err := p.pool.Do(ctx, ch.Query{
				Body:   sql,
				Result: proto.Results{{Name: "checksum", Data: &col}},
				OnResult: func(_ context.Context, block proto.Block) error {
					if block.Rows > 0 {
						*ptr = col.Row(0)
					}
					return nil
				},
			})
			return err
		}
	}
	return fmt.Errorf("QueryRow: unsupported dest type")
}

// queryJSON executes a SQL query via HTTP and unmarshals the first JSONEachRow result.
func (p *Pool) queryJSON(ctx context.Context, sql string, dest interface{}) error {
	u := strings.TrimRight(p.httpAddr, "/") + "/?database=" + p.Schema.Database + "&default_format=JSONEachRow"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))
	if err != nil {
		return err
	}
	if p.httpUser != "" {
		req.SetBasicAuth(p.httpUser, p.httpPassword)
	}
	resp, err := p.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	dec := json.NewDecoder(resp.Body)
	if dec.More() {
		return dec.Decode(dest)
	}
	return fmt.Errorf("no rows returned")
}

// extractParam gets a param value from QueryParams by name (without param_ prefix).
func extractParam(params *chclient.QueryParams, name string) string {
	if params == nil {
		return ""
	}
	v := params.URLValues().Get("param_" + name)
	return v
}

// extractMatchers extracts label matchers (lk0/lv0/lo0, lk1/lv1/lo1...) from params.
func extractMatchers(params *chclient.QueryParams) []LabelMatcher {
	if params == nil {
		return nil
	}
	vals := params.URLValues()
	var matchers []LabelMatcher
	for i := 0; ; i++ {
		is := strconv.Itoa(i)
		lk := vals.Get("param_lk" + is)
		lv := vals.Get("param_lv" + is)
		lo := vals.Get("param_lo" + is)
		if lk == "" {
			break
		}
		if lo == "" {
			lo = "=" // fallback
		}
		matchers = append(matchers, LabelMatcher{Name: lk, Op: lo, Value: lv})
	}
	return matchers
}

// inlineParams replaces {name:Type} placeholders with actual values.
func inlineParams(sql string, params *chclient.QueryParams) string {
	if params == nil {
		return sql
	}
	result := sql
	for key, values := range params.URLValues() {
		if len(values) == 0 {
			continue
		}
		name := key
		if strings.HasPrefix(name, "param_") {
			name = name[6:]
		}
		val := values[0]
		strP := "{" + name + ":String}"
		intP := "{" + name + ":Int64}"
		if strings.Contains(result, strP) {
			result = strings.ReplaceAll(result, strP, "'"+strings.ReplaceAll(val, "'", "\\'")+"'")
		}
		if strings.Contains(result, intP) {
			result = strings.ReplaceAll(result, intP, val)
		}
	}
	return result
}

// Ping establishes a connection and warms up the pool.
func (p *Pool) Ping(ctx context.Context) error {
	var col proto.ColUInt8
	return p.pool.Do(ctx, ch.Query{
		Body:   "SELECT 1",
		Result: proto.Results{{Name: "1", Data: &col}},
		OnResult: func(_ context.Context, _ proto.Block) error { return nil },
	})
}

// buildFingerprintIN builds an AND ... IN (...) clause for UInt64 fingerprints.
func buildFingerprintIN(fps []string, fpColumn string) string {
	if len(fps) == 0 {
		return ""
	}
	return "AND " + fpColumn + " IN (" + strings.Join(fps, ",") + ")"
}

// Close closes the pool.
func (p *Pool) Close() {
	p.pool.Close()
}
