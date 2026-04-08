package config

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// CHConfig holds ClickHouse connection settings (shared by all binaries).
type CHConfig struct {
	HTTPAddr   string `yaml:"http_addr"`
	NativeAddr string `yaml:"native_addr"`
	Database   string `yaml:"database"`
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
}

// SchemaConfig mirrors promql2chsql schema configuration (shared by all binaries).
type SchemaConfig struct {
	SamplesTable    string       `yaml:"samples_table"`
	TimeSeriesTable string       `yaml:"time_series_table"`
	Columns         ColumnConfig `yaml:"columns"`
	LabelsType      string       `yaml:"labels_type"`
}

// ColumnConfig maps column names.
type ColumnConfig struct {
	MetricName  string `yaml:"metric_name"`
	Timestamp   string `yaml:"timestamp"`
	Value       string `yaml:"value"`
	Fingerprint string `yaml:"fingerprint"`
	Labels      string `yaml:"labels"`
}

// Duration wraps time.Duration for YAML unmarshaling from strings like "2d", "90d", "5m".
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	// Support day notation: "2d" → 48h, "90d" → 2160h
	if len(s) > 1 && s[len(s)-1] == 'd' {
		var days int
		if _, err := fmt.Sscanf(s, "%dd", &days); err == nil {
			d.Duration = time.Duration(days) * 24 * time.Hour
			return nil
		}
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = dur
	return nil
}

func (d Duration) String() string {
	hours := d.Duration.Hours()
	if hours >= 24 && int(hours)%24 == 0 {
		return fmt.Sprintf("%dd", int(hours)/24)
	}
	return d.Duration.String()
}

// DownsamplingConfig defines Thanos-style downsampling tiers.
type DownsamplingConfig struct {
	Enabled      bool         `yaml:"enabled"`
	RawRetention Duration     `yaml:"raw_retention"`
	Tiers        []TierConfig `yaml:"tiers"`
}

// TierConfig defines a single downsampling tier.
type TierConfig struct {
	Name         string   `yaml:"name"`
	Resolution   Duration `yaml:"resolution"`
	Table        string   `yaml:"table"`
	CompactAfter Duration `yaml:"compact_after"`
	Retention    Duration `yaml:"retention"`
	MinStep      Duration `yaml:"min_step"`
}

// Validate checks downsampling configuration for correctness.
func (d *DownsamplingConfig) Validate() error {
	if !d.Enabled {
		return nil
	}
	if len(d.Tiers) == 0 {
		return fmt.Errorf("downsampling enabled but no tiers defined")
	}
	if d.RawRetention.Duration <= d.Tiers[0].CompactAfter.Duration {
		return fmt.Errorf(
			"raw_retention (%s) must be > tiers[0].compact_after (%s): "+
				"raw data would be deleted before MV can process it",
			d.RawRetention, d.Tiers[0].CompactAfter,
		)
	}
	for i := 1; i < len(d.Tiers); i++ {
		if d.Tiers[i].CompactAfter.Duration <= d.Tiers[i-1].CompactAfter.Duration {
			return fmt.Errorf(
				"tier[%d].compact_after (%s) must be > tier[%d].compact_after (%s)",
				i, d.Tiers[i].CompactAfter, i-1, d.Tiers[i-1].CompactAfter,
			)
		}
		if d.Tiers[i].MinStep.Duration <= d.Tiers[i-1].MinStep.Duration {
			return fmt.Errorf(
				"tier[%d].min_step (%s) must be > tier[%d].min_step (%s)",
				i, d.Tiers[i].MinStep, i-1, d.Tiers[i-1].MinStep,
			)
		}
	}
	return nil
}

// SelectTier returns the best tier for a given step, or nil (raw).
func (d *DownsamplingConfig) SelectTier(step time.Duration) *TierConfig {
	if !d.Enabled {
		return nil
	}
	var selected *TierConfig
	for i := range d.Tiers {
		if step >= d.Tiers[i].MinStep.Duration {
			selected = &d.Tiers[i]
		}
	}
	return selected
}

// QuerySegment defines a time range to read from a specific source.
type QuerySegment struct {
	Source string      // "raw", "5m", "1h" etc.
	Table  string      // "samples", "samples_5m", "samples_1h"
	IsRaw  bool        // true for raw samples
	Tier   *TierConfig // nil for raw
	Start  time.Time
	End    time.Time
}

// QuerySegments returns time-range segments for UNION ALL query routing.
func (d *DownsamplingConfig) QuerySegments(
	queryStart, queryEnd time.Time,
	selectedTier *TierConfig,
) []QuerySegment {
	if !d.Enabled || selectedTier == nil {
		return []QuerySegment{{
			Source: "raw",
			Table:  "samples",
			IsRaw:  true,
			Start:  queryStart,
			End:    queryEnd,
		}}
	}

	now := time.Now()
	var segments []QuerySegment

	// Overlap: extend raw segment 2h before compact_after boundary
	// to cover the gap where MV REFRESH may not have produced data yet.
	const rawOverlap = 2 * time.Hour

	tierBoundaries := make([]time.Time, len(d.Tiers)+1)
	for i, tier := range d.Tiers {
		tierBoundaries[i] = now.Add(-tier.CompactAfter.Duration - rawOverlap)
	}
	tierBoundaries[len(d.Tiers)] = now

	segStart := queryStart
	for i := len(d.Tiers) - 1; i >= 0; i-- {
		tier := d.Tiers[i]
		boundary := tierBoundaries[i]

		if !segStart.Before(boundary) || !segStart.Before(queryEnd) {
			continue
		}
		segEnd := boundary
		if segEnd.After(queryEnd) {
			segEnd = queryEnd
		}

		// Only use tiers with resolution <= selectedTier resolution
		if tier.Resolution.Duration <= selectedTier.Resolution.Duration {
			segments = append(segments, QuerySegment{
				Source: tier.Name,
				Table:  tier.Table,
				IsRaw:  false,
				Tier:   &d.Tiers[i],
				Start:  segStart,
				End:    segEnd,
			})
		}
		segStart = segEnd
	}

	// Raw segment for newest data
	if segStart.Before(queryEnd) {
		segments = append(segments, QuerySegment{
			Source: "raw",
			Table:  "samples",
			IsRaw:  true,
			Start:  segStart,
			End:    queryEnd,
		})
	}

	return segments
}
