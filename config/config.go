package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	Schema     SchemaConfig     `yaml:"schema"`
	Prometheus PrometheusConfig `yaml:"prometheus"`
	Output     OutputConfig     `yaml:"output"`
}

type ClickHouseConfig struct {
	Addr         string        `yaml:"addr"`
	Database     string        `yaml:"database"`
	Username     string        `yaml:"username"`
	Password     string        `yaml:"password"`
	QueryTimeout time.Duration `yaml:"query_timeout"`
	MaxOpenConns int           `yaml:"max_open_conns"`
}

type SchemaConfig struct {
	SamplesTable     string             `yaml:"samples_table"`
	TimeSeriesTable  string             `yaml:"time_series_table"`
	Columns          ColumnConfig       `yaml:"columns"`
	LabelsType       string             `yaml:"labels_type"`
	JSONExtractFunc  string             `yaml:"json_extract_function"`
	ExtractedColumns []ExtractedColumn  `yaml:"extracted_columns"`
	Downsampling     DownsamplingConfig `yaml:"downsampling"`
	TimestampIsInt   bool               `yaml:"timestamp_is_int"` // true if timestamp column is Int64 (unix_milli), false if DateTime64
}

type DownsamplingConfig struct {
	Enabled bool             `yaml:"enabled"`
	Tiers   []DownsampleTier `yaml:"tiers"`
}

type DownsampleTier struct {
	Name           string        `yaml:"name"`
	Table          string        `yaml:"table"`
	TimeColumn     string        `yaml:"time_column"`
	Resolution     time.Duration `yaml:"resolution"`
	MinStep        time.Duration `yaml:"min_step"`
	MaxAge         time.Duration `yaml:"max_age"`
	SupportedFuncs []string      `yaml:"supported_funcs"`
}

// SelectTier picks the best MV tier for a given step and query range.
// Returns (table, tier). tier==nil means use raw samples.
func (c *Config) SelectTier(step, queryRange time.Duration) (string, *DownsampleTier) {
	if !c.Schema.Downsampling.Enabled || queryRange <= 0 {
		return c.Schema.SamplesTable, nil
	}
	var best *DownsampleTier
	for i := range c.Schema.Downsampling.Tiers {
		t := &c.Schema.Downsampling.Tiers[i]
		if step >= t.MinStep && queryRange <= t.MaxAge {
			if best == nil || t.MinStep > best.MinStep {
				best = t
			}
		}
	}
	if best == nil {
		return c.Schema.SamplesTable, nil
	}
	return best.Table, best
}

type ColumnConfig struct {
	MetricName  string `yaml:"metric_name"`
	Timestamp   string `yaml:"timestamp"`
	Value       string `yaml:"value"`
	Fingerprint string `yaml:"fingerprint"`
	Labels      string `yaml:"labels"`
}

type ExtractedColumn struct {
	Label  string `yaml:"label"`
	Column string `yaml:"column"`
}

type PrometheusConfig struct {
	StalenessSeconds int           `yaml:"staleness_seconds"`
	MaxSamples       int           `yaml:"max_samples"`
	MaxSeries        int           `yaml:"max_series"`
	DefaultStep      time.Duration `yaml:"default_step"`
}

type OutputConfig struct {
	Format  string `yaml:"format"`
	ShowSQL bool   `yaml:"show_sql"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return defaults(), nil
		}
		return nil, err
	}
	cfg := defaults()
	return cfg, yaml.Unmarshal(data, cfg)
}

func defaults() *Config {
	cfg := &Config{}
	cfg.ClickHouse.Addr = "http://localhost:8123"
	cfg.ClickHouse.Database = "default"
	cfg.ClickHouse.QueryTimeout = 30 * time.Second
	cfg.ClickHouse.MaxOpenConns = 10
	cfg.Schema.SamplesTable = "samples"
	cfg.Schema.TimeSeriesTable = "time_series"
	cfg.Schema.Columns.MetricName = "metric_name"
	cfg.Schema.Columns.Timestamp = "unix_milli"
	cfg.Schema.Columns.Value = "value"
	cfg.Schema.Columns.Fingerprint = "fingerprint"
	cfg.Schema.Columns.Labels = "labels"
	cfg.Schema.LabelsType = "json"
	cfg.Schema.JSONExtractFunc = "JSONExtractString"
	cfg.Prometheus.StalenessSeconds = 300
	cfg.Prometheus.MaxSamples = 50_000_000
	cfg.Prometheus.MaxSeries = 10_000
	cfg.Prometheus.DefaultStep = 60 * time.Second
	cfg.Output.Format = "table"
	return cfg
}
