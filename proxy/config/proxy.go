package config

import (
	"os"
	"time"

	promqlcfg "github.com/PromClick/PromClick/config"
	"gopkg.in/yaml.v3"
)

// Config holds the proxy configuration.
type Config struct {
	ListenAddr   string             `yaml:"listen_addr"`
	QueryTimeout time.Duration      `yaml:"query_timeout"`
	ClickHouse   CHConfig           `yaml:"clickhouse"`
	Cache        CacheConfig        `yaml:"cache"`
	Labels       LabelsConfig       `yaml:"labels"`
	CORS         CORSConfig         `yaml:"cors"`
	Schema       SchemaConfig       `yaml:"schema"`
	Downsampling DownsamplingConfig `yaml:"downsampling"`
}

// CacheConfig holds optional caching settings.
type CacheConfig struct {
	Enabled      bool          `yaml:"enabled"`
	MaxSize      int           `yaml:"max_size"`
	TTL          time.Duration `yaml:"ttl"`
	MaxFreshness time.Duration `yaml:"max_freshness"`
}

// LabelsConfig controls in-memory label cache.
type LabelsConfig struct {
	CacheEnabled   bool          `yaml:"cache_enabled"`
	CacheTTL       time.Duration `yaml:"cache_ttl"`
	CacheMaxSeries int           `yaml:"cache_max_series"`
}

// CORSConfig holds CORS settings.
type CORSConfig struct {
	AllowOrigin string `yaml:"allow_origin"`
}

// Load reads the YAML config file and returns a Config with defaults.
func Load(path string) (*Config, error) {
	cfg := defaults()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		ListenAddr:   ":9090",
		QueryTimeout: 2 * time.Minute,
		ClickHouse: CHConfig{
			HTTPAddr: "http://localhost:8123",
			Database: "metrics",
			User:     "default",
			Password: "",
		},
		Cache: CacheConfig{
			Enabled:      false,
			MaxSize:      1000,
			TTL:          60 * time.Second,
			MaxFreshness: 60 * time.Second,
		},
		Labels: LabelsConfig{
			CacheEnabled:   true,
			CacheTTL:       60 * time.Second,
			CacheMaxSeries: 10000,
		},
		CORS: CORSConfig{
			AllowOrigin: "*",
		},
		Schema: SchemaConfig{
			SamplesTable:    "samples",
			TimeSeriesTable: "time_series",
			Columns: ColumnConfig{
				MetricName:  "metric_name",
				Timestamp:   "unix_milli",
				Value:       "value",
				Fingerprint: "fingerprint",
				Labels:      "labels",
			},
			LabelsType: "json",
		},
		Downsampling: DownsamplingConfig{
			Enabled: false,
		},
	}
}

// ToPromqlConfig converts the proxy config to the promql2chsql Config format.
func (c *Config) ToPromqlConfig() *promqlcfg.Config {
	return &promqlcfg.Config{
		ClickHouse: promqlcfg.ClickHouseConfig{
			Addr:         c.ClickHouse.HTTPAddr,
			Database:     c.ClickHouse.Database,
			Username:     c.ClickHouse.User,
			Password:     c.ClickHouse.Password,
			QueryTimeout: c.QueryTimeout,
			MaxOpenConns: 10,
		},
		Schema: promqlcfg.SchemaConfig{
			SamplesTable:    c.Schema.SamplesTable,
			TimeSeriesTable: c.Schema.TimeSeriesTable,
			Columns: promqlcfg.ColumnConfig{
				MetricName:  c.Schema.Columns.MetricName,
				Timestamp:   c.Schema.Columns.Timestamp,
				Value:       c.Schema.Columns.Value,
				Fingerprint: c.Schema.Columns.Fingerprint,
				Labels:      c.Schema.Columns.Labels,
			},
			LabelsType:     c.Schema.LabelsType,
			TimestampIsInt: true,
		},
		Prometheus: promqlcfg.PrometheusConfig{
			StalenessSeconds: 300,
			MaxSamples:       50_000_000,
			MaxSeries:        10_000,
			DefaultStep:      60 * time.Second,
		},
		Output: promqlcfg.OutputConfig{
			Format:  "json",
			ShowSQL: false,
		},
	}
}
