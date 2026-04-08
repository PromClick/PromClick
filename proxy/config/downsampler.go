package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// DownsamplerConfig holds configuration for the standalone downsampler binary.
type DownsamplerConfig struct {
	ClickHouse   CHConfig           `yaml:"clickhouse"`
	Schema       SchemaConfig       `yaml:"schema"`
	Downsampling DownsamplingConfig `yaml:"downsampling"`
	Daemon       bool               `yaml:"daemon"`
	Interval     time.Duration      `yaml:"interval"`
}

// LoadDownsampler reads downsampler YAML config with defaults.
func LoadDownsampler(path string) (*DownsamplerConfig, error) {
	cfg := downsamplerDefaults()
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

func downsamplerDefaults() *DownsamplerConfig {
	return &DownsamplerConfig{
		ClickHouse: CHConfig{
			HTTPAddr: "http://localhost:8123",
			Database: "metrics",
			User:     "default",
			Password: "",
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
		Daemon:   false,
		Interval: 1 * time.Hour,
	}
}
