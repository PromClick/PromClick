package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hinskii/promclick/config"
	"github.com/hinskii/promclick/types"
)

// CHError represents an error from ClickHouse HTTP interface.
type CHError struct {
	StatusCode int
	Message    string
}

func (e *CHError) Error() string {
	return fmt.Sprintf("ClickHouse HTTP %d: %s", e.StatusCode, e.Message)
}

// SeriesData holds samples and metadata for one series.
type SeriesData struct {
	Labels  map[string]string
	Samples []types.Sample
}

type Client struct {
	cfg  config.ClickHouseConfig
	http *http.Client
}

func NewClient(cfg config.ClickHouseConfig) *Client {
	return &Client{
		cfg: cfg,
		http: &http.Client{
			Timeout: cfg.QueryTimeout,
			Transport: &http.Transport{
				MaxIdleConns:    cfg.MaxOpenConns,
				IdleConnTimeout: 90 * time.Second,
			},
		},
	}
}

func (c *Client) buildRequest(ctx context.Context, sql string, params *QueryParams) *http.Request {
	qv := params.URLValues()
	qv.Set("default_format", "JSONEachRow")
	qv.Set("database", c.cfg.Database)

	u := strings.TrimRight(c.cfg.Addr, "/") + "/?" + qv.Encode()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))

	if c.cfg.Username != "" {
		req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	}
	req.Header.Set("Content-Type", "text/plain")
	return req
}

// FetchSeriesData executes SQL and returns map[fingerprint_string]*SeriesData with labels.
// Uses string keys for fingerprint to support both UUID and UInt64 columns.
func (c *Client) FetchSeriesData(ctx context.Context,
	sql string, params *QueryParams) (map[string]*SeriesData, error) {

	req := c.buildRequest(ctx, sql, params)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &CHError{StatusCode: resp.StatusCode, Message: string(body)}
	}

	type Row struct {
		Fingerprint string            `json:"fingerprint"`
		Ts          json.RawMessage   `json:"ts"`
		Value       float64           `json:"value"`
		Labels      map[string]string `json:"labels"`
	}

	result := make(map[string]*SeriesData)
	dec := json.NewDecoder(resp.Body)

	for dec.More() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		var row Row
		if err := dec.Decode(&row); err != nil {
			return nil, fmt.Errorf("decode row: %w", err)
		}

		// Parse timestamp: could be int64 (unix_milli) or string (DateTime64)
		tsMs := parseTimestamp(row.Ts)

		sd, ok := result[row.Fingerprint]
		if !ok {
			sd = &SeriesData{Labels: row.Labels}
			result[row.Fingerprint] = sd
		}
		sd.Samples = append(sd.Samples, types.Sample{
			Timestamp: tsMs,
			Value:     row.Value,
		})
	}
	return result, nil
}

// parseTimestamp handles numeric (Int64 ms), string numeric ("1234567890000"),
// and string DateTime64 ("2024-01-01 00:00:00.000") timestamps.
func parseTimestamp(raw json.RawMessage) int64 {
	// Try as number first
	var n float64
	if json.Unmarshal(raw, &n) == nil {
		return int64(n)
	}
	// Try as string
	var s string
	if json.Unmarshal(raw, &s) == nil {
		// Try as numeric string first
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
		// Try date formats
		for _, layout := range []string{
			"2006-01-02 15:04:05.000",
			"2006-01-02 15:04:05",
			time.RFC3339Nano,
			time.RFC3339,
		} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UnixMilli()
			}
		}
	}
	return 0
}
