package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// WriterConfig holds configuration for the standalone writer binary.
type WriterConfig struct {
	ListenAddr string       `yaml:"listen_addr"`
	ClickHouse CHConfig     `yaml:"clickhouse"`
	Schema     SchemaConfig `yaml:"schema"`
	Write      WriteSettings `yaml:"write"`
}

// WriteSettings controls the remote_write batch insertion.
type WriteSettings struct {
	BatchSize     int           `yaml:"batch_size"`
	QueueSize     int           `yaml:"queue_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
}

// LoadWriter reads writer YAML config with defaults.
func LoadWriter(path string) (*WriterConfig, error) {
	cfg := writerDefaults()
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

func writerDefaults() *WriterConfig {
	return &WriterConfig{
		ListenAddr: ":9091",
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
		Write: WriteSettings{
			BatchSize:     10000,
			QueueSize:     100000,
			FlushInterval: 5 * time.Second,
		},
	}
}
