package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// LabelCache caches time_series labels in-memory to eliminate JOIN per query.
type LabelCache struct {
	mu          sync.RWMutex
	data        map[string]map[string]string // fingerprint → labels
	metricIndex map[string][]string          // metric_name → []fingerprint
	loaded      bool
	lastRefresh time.Time
	ttl         time.Duration
	maxSeries   int

	httpClient *http.Client
	httpAddr   string
	database   string
	user       string
	password   string
	table      string
	fpCol      string
	mnCol      string
	lblCol     string
}

// LabelMatcher for Go-side label filtering.
type LabelMatcher struct {
	Name  string
	Op    string // "=", "!=", "=~", "!~"
	Value string
}

// NewLabelCache creates a new label cache.
func NewLabelCache(ttl time.Duration, maxSeries int,
	httpClient *http.Client,
	httpAddr, database, user, password, table, fpCol, mnCol, lblCol string) *LabelCache {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &LabelCache{
		data:        make(map[string]map[string]string),
		metricIndex: make(map[string][]string),
		ttl:         ttl,
		maxSeries:   maxSeries,
		httpClient:  httpClient,
		httpAddr:    httpAddr,
		database:    database,
		user:        user,
		password:    password,
		table:       table,
		fpCol:       fpCol,
		mnCol:       mnCol,
		lblCol:      lblCol,
	}
}

// IsLoaded returns true if cache has data.
func (c *LabelCache) IsLoaded() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loaded
}

// Size returns number of cached fingerprints.
func (c *LabelCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

// GetLabels returns labels for a fingerprint.
func (c *LabelCache) GetLabels(fp string) (map[string]string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	l, ok := c.data[fp]
	return l, ok
}

// GetFingerprints returns fingerprints for a metric, filtered by matchers in Go.
// Returns (fps, true) if cache usable, (nil, false) to fallback to JOIN.
func (c *LabelCache) GetFingerprints(metricName string, matchers []LabelMatcher) ([]string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fps, ok := c.metricIndex[metricName]
	if !ok {
		return []string{}, true // metric unknown → empty result
	}
	if c.maxSeries > 0 && len(fps) > c.maxSeries {
		return nil, false // too many → fallback
	}
	if len(matchers) == 0 {
		cp := make([]string, len(fps))
		copy(cp, fps)
		return cp, true
	}

	var result []string
	for _, fp := range fps {
		if matchAll(c.data[fp], matchers) {
			result = append(result, fp)
		}
	}
	// Sort for better CH IN clause performance
	sort.Strings(result)
	return result, true
}

// Regex cache — avoid recompiling per call
var regexCache sync.Map

func cachedRegexp(pattern string) (*regexp.Regexp, error) {
	if v, ok := regexCache.Load(pattern); ok {
		return v.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile("^(?:" + pattern + ")$")
	if err != nil {
		return nil, err
	}
	regexCache.Store(pattern, re)
	return re, nil
}

func matchAll(labels map[string]string, matchers []LabelMatcher) bool {
	if labels == nil {
		return false
	}
	for _, m := range matchers {
		v := labels[m.Name]
		switch m.Op {
		case "=":
			if v != m.Value {
				return false
			}
		case "!=":
			if v == m.Value {
				return false
			}
		case "=~":
			re, err := cachedRegexp(m.Value)
			if err != nil || !re.MatchString(v) {
				return false
			}
		case "!~":
			re, err := cachedRegexp(m.Value)
			if err != nil || re.MatchString(v) {
				return false
			}
		}
	}
	return true
}

// Refresh loads all labels from time_series via HTTP.
func (c *LabelCache) Refresh(ctx context.Context) error {
	sql := fmt.Sprintf(
		"SELECT toString(%s) AS fp, %s AS mn, %s AS labels FROM %s ORDER BY unix_milli DESC LIMIT 1 BY %s, %s",
		c.fpCol, c.mnCol, c.lblCol, c.table, c.mnCol, c.fpCol)

	u := strings.TrimRight(c.httpAddr, "/") + "/?database=" + c.database + "&default_format=JSONEachRow"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))
	if err != nil {
		return err
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("label cache: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("label cache HTTP %d: %s", resp.StatusCode, body)
	}

	type row struct {
		FP     string `json:"fp"`
		MN     string `json:"mn"`
		Labels string `json:"labels"`
	}

	newData := make(map[string]map[string]string)
	newIndex := make(map[string][]string)

	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var r row
		if err := dec.Decode(&r); err != nil {
			slog.Warn("label cache: skip malformed row", "error", err)
			continue
		}
		var lblMap map[string]string
		if err := json.Unmarshal([]byte(r.Labels), &lblMap); err != nil {
			slog.Warn("label cache: skip invalid labels JSON", "fp", r.FP, "error", err)
			continue
		}
		newData[r.FP] = lblMap
		newIndex[r.MN] = append(newIndex[r.MN], r.FP)
	}

	c.mu.Lock()
	c.data = newData
	c.metricIndex = newIndex
	c.loaded = true
	c.lastRefresh = time.Now()
	c.mu.Unlock()
	return nil
}

// StartBackgroundRefresh refreshes cache every TTL in background.
func (c *LabelCache) StartBackgroundRefresh(ctx context.Context) {
	go func() {
		t := time.NewTicker(c.ttl)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				if err := c.Refresh(refreshCtx); err != nil {
					slog.Warn("label cache refresh failed", "error", err)
				}
				cancel()
			case <-ctx.Done():
				return
			}
		}
	}()
}
