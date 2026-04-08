package translator

import (
	"fmt"
	"strings"

	"github.com/hinskii/promclick/clickhouse"
	"github.com/hinskii/promclick/config"
)

// LabelMatcher represents a PromQL label matcher for SQL generation.
type LabelMatcher struct {
	Name string
	Op   string // "=", "!=", "=~", "!~"
	Val  string
}

// VectorMatching describes how two vectors should be matched in binary ops.
type VectorMatching struct {
	Card           string   // "one-to-one", "many-to-one", "one-to-many"
	MatchingLabels []string // on(...) or ignoring(...)
	On             bool     // true = on(), false = ignoring()
	Include        []string // group_left/group_right labels
}

// MathStep represents a math wrapper function applied after aggregation.
type MathStep struct {
	Fn    string  // "abs","ceil","clamp_min","sort", etc.
	Param float64 // for clamp_min/clamp_max/round
	Param2 float64 // for clamp (min, max)
	SortLabels []string // for sort_by_label
}

// AggStep represents one level of aggregation in a chain.
type AggStep struct {
	Op       string   // "sum","avg","topk","count", etc.
	Param    float64  // topk(k), quantile(φ)
	Label    string   // count_values label
	Grouping []string // by/without labels
	Without  bool
}

// SQLPlan is the intermediate representation between PromQL AST and SQL.
type SQLPlan struct {
	MetricName     string
	Matchers       []LabelMatcher
	DataStartMs    int64
	DataEndMs      int64
	FilterStaleNaN bool
	AST            string // string representation of the AST for --explain
	Cfg            *config.Config

	// Expression type and function info
	ExprType      string // "vector"|"matrix"|"call"|"aggregate"|"binary"|"scalar"
	FuncName      string // "rate","irate","sum_over_time", etc.
	InnerFuncName string // preserved inner function name (e.g. "rate" inside histogram_quantile)
	RangeMs       int64  // range duration in ms (for MatrixSelector)

	// Inner nested function (e.g. rate inside clamp_max)
	Inner *SQLPlan

	// Aggregation chain — innermost first, outermost last.
	// e.g. topk(3, count(sum by(x)(...))) → [sum by(x), count, topk(3)]
	AggChain []AggStep

	// Legacy single-level fields (used by applyAggregation if AggChain empty)
	AggOp    string   // "sum","avg","topk", etc.
	AggParam float64  // topk(k,v) → k; quantile(φ,v) → φ
	AggLabel string   // count_values(label, v) → label
	Grouping []string // by/without labels
	Without  bool

	// Binary ops
	LHS            *SQLPlan
	RHS            *SQLPlan
	BinaryOp       string
	ReturnBool     bool
	VectorMatching *VectorMatching

	// Label functions
	LabelFuncArgs []string

	// Sort
	SortLabels []string

	// Math chain — outer-to-inner math wrappers applied AFTER aggregation
	// e.g. ceil(abs(sum(rate(...)))) → MathChain=["ceil","abs"]
	MathChain []MathStep

	// Smoothing
	SmoothingTF float64

	// Scalar
	IsScalar  bool
	ScalarVal float64

	// Absent
	AbsentMatchers []LabelMatcher

	// Offset
	OffsetMs int64

	// Scalar sides for binary ops
	IsScalarRHS bool
	ScalarRHS   float64
	IsScalarLHS bool
	ScalarLHS   float64
}

// Render generates the SQL query string and params.
// Uses JOIN with time_series table to fetch labels alongside samples.
func (p *SQLPlan) Render() (string, *clickhouse.QueryParams) {
	// Binary plan: render both sides
	if p.ExprType == "binary" && p.LHS != nil && p.RHS != nil {
		lSQL, lParams := p.LHS.Render()
		rSQL, _ := p.RHS.Render()
		return lSQL + "\n-- RHS:\n" + rSQL, lParams
	}

	// Scalar literal
	if p.IsScalar {
		return fmt.Sprintf("SELECT toFloat64(%g) AS value", p.ScalarVal), clickhouse.NewParams()
	}

	params := clickhouse.NewParams()
	cfg := p.Cfg
	if cfg == nil {
		return "-- no config", params
	}
	cols := cfg.Schema.Columns

	var b strings.Builder

	// SELECT with JOIN to get labels
	fmt.Fprintf(&b, "SELECT toString(s.%s) AS fingerprint, s.%s AS ts, s.%s AS value, t.%s AS labels",
		cols.Fingerprint, cols.Timestamp, cols.Value, cols.Labels)
	fmt.Fprintf(&b, "\nFROM %s AS s", cfg.Schema.SamplesTable)
	fmt.Fprintf(&b, "\nINNER JOIN (SELECT * FROM %s FINAL) AS t ON s.%s = t.%s",
		cfg.Schema.TimeSeriesTable, cols.Fingerprint, cols.Fingerprint)
	fmt.Fprintf(&b, "\nWHERE t.%s = {metricName:String}", cols.MetricName)
	if cfg.Schema.TimestampIsInt {
		fmt.Fprintf(&b, "\n  AND s.%s > {dataStart:Int64}", cols.Timestamp)
		fmt.Fprintf(&b, "\n  AND s.%s <= {dataEnd:Int64}", cols.Timestamp)
	} else {
		fmt.Fprintf(&b, "\n  AND toUnixTimestamp64Milli(s.%s) > {dataStart:Int64}", cols.Timestamp)
		fmt.Fprintf(&b, "\n  AND toUnixTimestamp64Milli(s.%s) <= {dataEnd:Int64}", cols.Timestamp)
	}

	params.AddString("metricName", p.MetricName)
	params.AddInt64("dataStart", p.DataStartMs)
	params.AddInt64("dataEnd", p.DataEndMs)

	// Label filters on time_series table
	for i, m := range p.Matchers {
		paramName := fmt.Sprintf("lv%d", i)
		labelKey := fmt.Sprintf("lk%d", i)
		labelOp := fmt.Sprintf("lo%d", i)
		accessor := labelAccessor(cfg, m.Name, labelKey, paramName)
		params.AddString(paramName, m.Val)
		params.AddString(labelKey, m.Name)
		params.AddString(labelOp, m.Op)
		switch m.Op {
		case "=":
			fmt.Fprintf(&b, "\n  AND %s = {%s:String}", accessor, paramName)
		case "!=":
			fmt.Fprintf(&b, "\n  AND %s != {%s:String}", accessor, paramName)
		case "=~":
			fmt.Fprintf(&b, "\n  AND match(%s, {%s:String})", accessor, paramName)
		case "!~":
			fmt.Fprintf(&b, "\n  AND NOT match(%s, {%s:String})", accessor, paramName)
		}
	}

	if p.FilterStaleNaN {
		b.WriteString("\n  AND reinterpretAsUInt64(s.value) != 9218868437227405314")
	}

	fmt.Fprintf(&b, "\nORDER BY s.%s ASC, s.%s ASC", cols.Fingerprint, cols.Timestamp)

	return b.String(), params
}

// RenderMV generates SQL to read from a materialized view tier.
func (p *SQLPlan) RenderMV(cfg *config.Config, tier *config.DownsampleTier) (string, *clickhouse.QueryParams) {
	params := clickhouse.NewParams()
	cols := cfg.Schema.Columns

	params.AddString("metricName", p.MetricName)
	params.AddInt64("dataStart", p.DataStartMs)
	params.AddInt64("dataEnd", p.DataEndMs)

	mvCol := mvAggColumn(p.FuncName)
	mergeFn := mvAggMerge(p.FuncName)

	var b strings.Builder

	fmt.Fprintf(&b, "SELECT toString(m.%s) AS fingerprint", cols.Fingerprint)
	fmt.Fprintf(&b, ", toUnixTimestamp(m.%s) * 1000 AS ts", tier.TimeColumn)
	fmt.Fprintf(&b, ", %s(m.%s) AS value", mergeFn, mvCol)
	fmt.Fprintf(&b, ", t.%s AS labels", cols.Labels)
	fmt.Fprintf(&b, "\nFROM %s AS m", tier.Table)
	fmt.Fprintf(&b, "\nINNER JOIN (SELECT * FROM %s FINAL) AS t ON m.%s = t.%s",
		cfg.Schema.TimeSeriesTable, cols.Fingerprint, cols.Fingerprint)
	fmt.Fprintf(&b, "\nWHERE m.metric_name = {metricName:String}")
	fmt.Fprintf(&b, "\n  AND m.%s >= toDateTime({dataStart:Int64} / 1000)", tier.TimeColumn)
	fmt.Fprintf(&b, "\n  AND m.%s <  toDateTime({dataEnd:Int64} / 1000)", tier.TimeColumn)

	// Label filters
	for i, m := range p.Matchers {
		paramName := fmt.Sprintf("lv%d", i)
		labelKey := fmt.Sprintf("lk%d", i)
		labelOp := fmt.Sprintf("lo%d", i)
		accessor := labelAccessor(cfg, m.Name, labelKey, paramName)
		params.AddString(paramName, m.Val)
		params.AddString(labelKey, m.Name)
		params.AddString(labelOp, m.Op)
		switch m.Op {
		case "=":
			fmt.Fprintf(&b, "\n  AND %s = {%s:String}", accessor, paramName)
		case "!=":
			fmt.Fprintf(&b, "\n  AND %s != {%s:String}", accessor, paramName)
		case "=~":
			fmt.Fprintf(&b, "\n  AND match(%s, {%s:String})", accessor, paramName)
		case "!~":
			fmt.Fprintf(&b, "\n  AND NOT match(%s, {%s:String})", accessor, paramName)
		}
	}

	fmt.Fprintf(&b, "\nGROUP BY m.%s, m.metric_name, m.%s, t.%s",
		cols.Fingerprint, tier.TimeColumn, cols.Labels)
	fmt.Fprintf(&b, "\nORDER BY m.%s ASC, ts ASC", cols.Fingerprint)

	return b.String(), params
}

func mvAggColumn(funcName string) string {
	switch funcName {
	case "min_over_time":
		return "min_val"
	case "max_over_time":
		return "max_val"
	case "sum_over_time":
		return "sum_val"
	case "count_over_time":
		return "count_val"
	case "last_over_time":
		return "last_val"
	default:
		return "avg_val"
	}
}

func mvAggMerge(funcName string) string {
	switch funcName {
	case "min_over_time":
		return "minMerge"
	case "max_over_time":
		return "maxMerge"
	case "sum_over_time":
		return "sumMerge"
	case "count_over_time":
		return "countMerge"
	case "last_over_time":
		return "argMaxMerge"
	default:
		return "avgMerge"
	}
}

// labelAccessor returns SQL expression to access a label value.
func labelAccessor(cfg *config.Config, labelName, keyParam, _ string) string {
	for _, col := range cfg.Schema.ExtractedColumns {
		if col.Label == labelName {
			return fmt.Sprintf("t.%s", col.Column)
		}
	}
	switch cfg.Schema.LabelsType {
	case "map":
		return fmt.Sprintf("t.%s[{%s:String}]", cfg.Schema.Columns.Labels, keyParam)
	default: // json
		return fmt.Sprintf("%s(t.%s, {%s:String})",
			cfg.Schema.JSONExtractFunc, cfg.Schema.Columns.Labels, keyParam)
	}
}
