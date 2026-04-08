package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hinskii/promclick/proxy/config"
)

// ErrRequiresRawSamples signals that the function cannot use downsampled data.
var ErrRequiresRawSamples = errors.New("function requires raw samples, fallback to raw query")

// BuildSegmentedQuery builds a UNION ALL query across tier segments.
// Window functions are ALWAYS applied after UNION ALL, never inside segments.
func BuildSegmentedQuery(
	fn string,
	metricType MetricType,
	metricName string,
	segments []config.QuerySegment,
	fingerprints []uint64,
	step time.Duration,
	predictDuration ...time.Duration,
) (string, error) {
	if len(segments) == 0 {
		return "", fmt.Errorf("no segments")
	}

	useCounter := IsCounterLike(metricName, metricType)

	switch fn {
	case "rate", "increase":
		if useCounter {
			return buildCounterRateUnion(segments, fingerprints, metricName, step, fn), nil
		}
		return "", ErrRequiresRawSamples

	case "avg_over_time", "min_over_time", "max_over_time", "sum_over_time", "count_over_time":
		return buildGaugeUnion(segments, fingerprints, metricName, step), nil

	case "irate":
		if useCounter {
			return buildIRateUnion(segments, fingerprints, metricName, step), nil
		}
		return "", ErrRequiresRawSamples

	case "deriv":
		return buildDerivUnion(segments, fingerprints, metricName, step), nil

	case "predict_linear":
		pd := 3600 * time.Second
		if len(predictDuration) > 0 {
			pd = predictDuration[0]
		}
		return buildPredictLinearUnion(segments, fingerprints, metricName, step, pd), nil

	default:
		return "", ErrRequiresRawSamples
	}
}

// buildCounterRateUnion — rate()/increase() with UNION ALL.
// Returns per-bucket data with first_time/last_time for extrapolation.
// Go-side sliding window eval handles step windowing (43ms for 1111 series).
func buildCounterRateUnion(
	segments []config.QuerySegment,
	fps []uint64,
	metric string,
	step time.Duration,
	fn string,
) string {
	var parts []string
	for _, seg := range segments {
		if seg.IsRaw {
			parts = append(parts, buildRawCounterSegment(seg, fps, metric))
		} else {
			parts = append(parts, buildTierCounterSegment(seg, fps, metric))
		}
	}

	union := strings.Join(parts, "\n    UNION ALL\n")

	return fmt.Sprintf(`
WITH all_segments AS (
    %s
)
SELECT
    fingerprint,
    toInt64(toUnixTimestamp(ts)) * 1000 AS step_ts,
    toFloat64(counter_total) AS value,
    toInt64(first_time) AS ft,
    toInt64(last_time) AS lt
FROM all_segments
ORDER BY fingerprint, step_ts`,
		union,
	)
}

// buildTierCounterSegment — counter_total from a downsampled tier.
func buildTierCounterSegment(seg config.QuerySegment, fps []uint64, metric string) string {
	return fmt.Sprintf(`
    SELECT fingerprint, ts,
        toFloat64(counter_total) AS counter_total,
        toInt64(first_time) AS first_time,
        toInt64(last_time) AS last_time
    FROM metrics.%s
    WHERE metric_name = '%s'
      AND fingerprint IN (%s)
      AND ts BETWEEN toDateTime('%s') AND toDateTime('%s')`,
		seg.Table, metric, uintSliceToSQL(fps),
		seg.Start.UTC().Format(time.DateTime),
		seg.End.UTC().Format(time.DateTime),
	)
}

// buildRawCounterSegment — raw samples pre-aggregated to bucket format with lagInFrame.
func buildRawCounterSegment(seg config.QuerySegment, fps []uint64, metric string) string {
	bucketExpr := "toStartOfFiveMinutes(toDateTime(intDiv(unix_milli, 1000)))"

	return fmt.Sprintf(`
    SELECT
        fingerprint,
        %s AS ts,
        sum(
            CASE
                WHEN rn = 1              THEN 0
                WHEN value >= prev_value THEN value - prev_value
                ELSE value
            END
        )                              AS counter_total,
        min(unix_milli)                AS first_time,
        max(unix_milli)                AS last_time
    FROM (
        SELECT
            fingerprint, unix_milli, value,
            lagInFrame(value, 1, 0) OVER (
                PARTITION BY fingerprint
                ORDER BY unix_milli ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS prev_value,
            row_number() OVER (
                PARTITION BY fingerprint
                ORDER BY unix_milli ASC
            ) AS rn
        FROM metrics.samples
        WHERE metric_name = '%s'
          AND fingerprint IN (%s)
          AND unix_milli >= toInt64(toUnixTimestamp(toDateTime('%s'))) * 1000
          AND unix_milli <  toInt64(toUnixTimestamp(toDateTime('%s'))) * 1000
        ORDER BY fingerprint, unix_milli
    )
    GROUP BY fingerprint, %s`,
		bucketExpr,
		metric, uintSliceToSQL(fps),
		seg.Start.UTC().Format(time.DateTime),
		seg.End.UTC().Format(time.DateTime),
		bucketExpr,
	)
}

// buildGaugeUnion — all gauge over_time functions with UNION ALL.
// Push-down: aggregates per step in SQL, returns 6 columns.
// Handler picks the right column based on function name — no Go-side windowed eval.
func buildGaugeUnion(
	segments []config.QuerySegment,
	fps []uint64,
	metric string,
	step ...time.Duration,
) string {
	var parts []string
	for _, seg := range segments {
		if seg.IsRaw {
			parts = append(parts, buildRawGaugeSegment(seg, fps, metric))
		} else {
			parts = append(parts, buildTierGaugeSegment(seg, fps, metric))
		}
	}

	union := strings.Join(parts, "\n    UNION ALL\n")

	// If step provided, GROUP BY step for push-down
	if len(step) > 0 && step[0] > 0 {
		stepExpr := startOfIntervalExpr("ts", step[0])
		return fmt.Sprintf(`
WITH all_segments AS (
    %s
)
SELECT fingerprint,
    toInt64(toUnixTimestamp(%s)) * 1000 AS step_ts,
    sum(val_sum) AS val_sum,
    sum(val_count) AS val_count,
    min(val_min) AS val_min,
    max(val_max) AS val_max
FROM all_segments
GROUP BY fingerprint, step_ts
ORDER BY fingerprint, step_ts`,
			union, stepExpr,
		)
	}

	return fmt.Sprintf(`
WITH all_segments AS (
    %s
)
SELECT fingerprint,
    toInt64(toUnixTimestamp(ts)) * 1000 AS step_ts,
    val_sum, val_count, val_min, val_max
FROM all_segments
ORDER BY fingerprint, step_ts`,
		union,
	)
}

// buildTierGaugeSegment — gauge aggregates from a downsampled tier.
// Cast SimpleAggregateFunction columns to plain types for ch-go compatibility.
func buildTierGaugeSegment(seg config.QuerySegment, fps []uint64, metric string) string {
	return fmt.Sprintf(`
    SELECT fingerprint, ts,
        toFloat64(val_sum) AS val_sum,
        toUInt64(val_count) AS val_count,
        toFloat64(val_min) AS val_min,
        toFloat64(val_max) AS val_max
    FROM metrics.%s
    WHERE metric_name = '%s'
      AND fingerprint IN (%s)
      AND ts BETWEEN toDateTime('%s') AND toDateTime('%s')`,
		seg.Table, metric, uintSliceToSQL(fps),
		seg.Start.UTC().Format(time.DateTime),
		seg.End.UTC().Format(time.DateTime),
	)
}

// buildRawGaugeSegment — raw samples pre-aggregated to bucket format for gauge.
func buildRawGaugeSegment(seg config.QuerySegment, fps []uint64, metric string) string {
	bucketExpr := "toStartOfFiveMinutes(toDateTime(intDiv(unix_milli, 1000)))"
	return fmt.Sprintf(`
    SELECT
        fingerprint,
        %s AS ts,
        sum(value)        AS val_sum,
        toUInt64(count()) AS val_count,
        min(value)        AS val_min,
        max(value)        AS val_max
    FROM metrics.samples
    WHERE metric_name = '%s'
      AND fingerprint IN (%s)
      AND unix_milli >= toInt64(toUnixTimestamp(toDateTime('%s'))) * 1000
      AND unix_milli <  toInt64(toUnixTimestamp(toDateTime('%s'))) * 1000
    GROUP BY fingerprint, %s`,
		bucketExpr, metric, uintSliceToSQL(fps),
		seg.Start.UTC().Format(time.DateTime),
		seg.End.UTC().Format(time.DateTime),
		bucketExpr,
	)
}

// --- irate() on downsampled ---

// buildIRateUnion — irate() using last_value per bucket with lag window function.
func buildIRateUnion(
	segments []config.QuerySegment,
	fps []uint64,
	metric string,
	step time.Duration,
) string {
	var parts []string
	for _, seg := range segments {
		if seg.IsRaw {
			parts = append(parts, buildRawIRateSegment(seg, fps, metric))
		} else {
			parts = append(parts, buildTierIRateSegment(seg, fps, metric))
		}
	}

	union := strings.Join(parts, "\n    UNION ALL\n")
	stepExpr := startOfIntervalExpr("ts", step)

	return fmt.Sprintf(`
WITH all_segments AS (
    %s
),
with_prev AS (
    SELECT
        fingerprint, ts,
        last_value, last_time,
        lagInFrame(last_value, 1, toFloat64(0)) OVER (
            PARTITION BY fingerprint
            ORDER BY ts ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS prev_last_value,
        lagInFrame(last_time, 1, toInt64(0)) OVER (
            PARTITION BY fingerprint
            ORDER BY ts ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS prev_last_time,
        row_number() OVER (
            PARTITION BY fingerprint
            ORDER BY ts ASC
        ) AS rn
    FROM all_segments
)
SELECT fingerprint,
    %s AS step_ts,
    CASE
        WHEN rn = 1                     THEN 0
        WHEN last_time = prev_last_time THEN 0
        WHEN last_value >= prev_last_value
            THEN (last_value - prev_last_value)
                 / ((last_time - prev_last_time) / 1000.0)
        ELSE last_value / ((last_time - prev_last_time) / 1000.0)
    END AS value
FROM with_prev
WHERE rn > 1
ORDER BY fingerprint, step_ts`,
		union, stepExpr,
	)
}

func buildTierIRateSegment(seg config.QuerySegment, fps []uint64, metric string) string {
	return fmt.Sprintf(`
    SELECT fingerprint, ts,
        argMaxMerge(last_value) AS last_value,
        max(last_time)          AS last_time
    FROM metrics.%s
    WHERE metric_name = '%s'
      AND fingerprint IN (%s)
      AND ts BETWEEN toDateTime('%s') AND toDateTime('%s')
    GROUP BY fingerprint, ts`,
		seg.Table, metric, uintSliceToSQL(fps),
		seg.Start.UTC().Format(time.DateTime),
		seg.End.UTC().Format(time.DateTime),
	)
}

func buildRawIRateSegment(seg config.QuerySegment, fps []uint64, metric string) string {
	bucketExpr := "toStartOfFiveMinutes(toDateTime(intDiv(unix_milli, 1000)))"
	return fmt.Sprintf(`
    SELECT fingerprint,
        %s AS ts,
        argMax(value, unix_milli) AS last_value,
        max(unix_milli)           AS last_time
    FROM metrics.samples
    WHERE metric_name = '%s'
      AND fingerprint IN (%s)
      AND unix_milli >= toInt64(toUnixTimestamp(toDateTime('%s'))) * 1000
      AND unix_milli <  toInt64(toUnixTimestamp(toDateTime('%s'))) * 1000
    GROUP BY fingerprint, %s`,
		bucketExpr, metric, uintSliceToSQL(fps),
		seg.Start.UTC().Format(time.DateTime),
		seg.End.UTC().Format(time.DateTime),
		bucketExpr,
	)
}

// --- deriv() on downsampled ---

// buildDerivUnion — deriv() using simpleLinearRegression on last_value per bucket.
func buildDerivUnion(
	segments []config.QuerySegment,
	fps []uint64,
	metric string,
	step time.Duration,
) string {
	var parts []string
	for _, seg := range segments {
		if seg.IsRaw {
			parts = append(parts, buildRawRegressionSegment(seg, fps, metric))
		} else {
			parts = append(parts, buildTierRegressionSegment(seg, fps, metric))
		}
	}

	union := strings.Join(parts, "\n    UNION ALL\n")
	stepExpr := startOfIntervalExpr("ts", step)

	return fmt.Sprintf(`
WITH all_segments AS (
    %s
),
regression AS (
    SELECT fingerprint,
        %s AS step_ts,
        simpleLinearRegression(last_value, ts_unix) AS reg
    FROM all_segments
    GROUP BY fingerprint, step_ts
)
SELECT fingerprint, step_ts,
    reg.1 AS value
FROM regression
ORDER BY fingerprint, step_ts`,
		union, stepExpr,
	)
}

// --- predict_linear() on downsampled ---

func buildPredictLinearUnion(
	segments []config.QuerySegment,
	fps []uint64,
	metric string,
	step time.Duration,
	predictDuration time.Duration,
) string {
	var parts []string
	for _, seg := range segments {
		if seg.IsRaw {
			parts = append(parts, buildRawRegressionSegment(seg, fps, metric))
		} else {
			parts = append(parts, buildTierRegressionSegment(seg, fps, metric))
		}
	}

	union := strings.Join(parts, "\n    UNION ALL\n")
	stepExpr := startOfIntervalExpr("ts", step)
	predictSecs := int(predictDuration.Seconds())

	return fmt.Sprintf(`
WITH all_segments AS (
    %s
),
regression AS (
    SELECT fingerprint,
        %s AS step_ts,
        simpleLinearRegression(last_value, ts_unix) AS reg,
        max(ts_unix) AS last_ts_unix
    FROM all_segments
    GROUP BY fingerprint, step_ts
)
SELECT fingerprint, step_ts,
    reg.2 + reg.1 * (last_ts_unix + %d) AS value
FROM regression
ORDER BY fingerprint, step_ts`,
		union, stepExpr, predictSecs,
	)
}

// --- Shared regression segments ---

func buildTierRegressionSegment(seg config.QuerySegment, fps []uint64, metric string) string {
	return fmt.Sprintf(`
    SELECT fingerprint, ts,
        argMaxMerge(last_value)  AS last_value,
        toUnixTimestamp(ts)      AS ts_unix
    FROM metrics.%s
    WHERE metric_name = '%s'
      AND fingerprint IN (%s)
      AND ts BETWEEN toDateTime('%s') AND toDateTime('%s')
    GROUP BY fingerprint, ts`,
		seg.Table, metric, uintSliceToSQL(fps),
		seg.Start.UTC().Format(time.DateTime),
		seg.End.UTC().Format(time.DateTime),
	)
}

func buildRawRegressionSegment(seg config.QuerySegment, fps []uint64, metric string) string {
	bucketExpr := "toStartOfFiveMinutes(toDateTime(intDiv(unix_milli, 1000)))"
	return fmt.Sprintf(`
    SELECT fingerprint,
        %s AS ts,
        argMax(value, unix_milli)  AS last_value,
        toUnixTimestamp(%s)        AS ts_unix
    FROM metrics.samples
    WHERE metric_name = '%s'
      AND fingerprint IN (%s)
      AND unix_milli >= toInt64(toUnixTimestamp(toDateTime('%s'))) * 1000
      AND unix_milli <  toInt64(toUnixTimestamp(toDateTime('%s'))) * 1000
    GROUP BY fingerprint, %s`,
		bucketExpr, bucketExpr,
		metric, uintSliceToSQL(fps),
		seg.Start.UTC().Format(time.DateTime),
		seg.End.UTC().Format(time.DateTime),
		bucketExpr,
	)
}

// uintSliceToSQL converts a []uint64 to a comma-separated SQL string.
func uintSliceToSQL(fps []uint64) string {
	if len(fps) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(len(fps) * 20) // uint64 max is 20 digits
	for i, fp := range fps {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(fp, 10))
	}
	return b.String()
}
