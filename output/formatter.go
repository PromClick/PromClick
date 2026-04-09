package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/PromClick/PromClick/types"
)

type Formatter interface {
	Format(w io.Writer, r *types.QueryResult)
}

func NewFormatter(format string) (Formatter, error) {
	switch format {
	case "table":
		return &Table{}, nil
	case "json":
		return &JSON{}, nil
	case "csv":
		return &CSV{}, nil
	case "sql":
		return &SQL{}, nil
	default:
		return nil, fmt.Errorf("unknown format: %s", format)
	}
}

// Table — ASCII tabela z tabwriter
type Table struct{}

func (f *Table) Format(w io.Writer, r *types.QueryResult) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	defer tw.Flush()
	switch r.Type {
	case "vector":
		fmt.Fprintln(tw, "LABELS\tTIMESTAMP\tVALUE")
		for _, s := range r.Vector {
			fmt.Fprintf(tw, "%s\t%s\t%s\n",
				labelsStr(s.Labels), fmtTs(s.T), fmtF(s.F))
		}
	case "matrix":
		fmt.Fprintln(tw, "LABELS\tTIMESTAMP\tVALUE")
		for _, series := range r.Matrix {
			lbl := labelsStr(series.Labels)
			for _, p := range series.Samples {
				fmt.Fprintf(tw, "%s\t%s\t%s\n", lbl, fmtTs(p.Timestamp), fmtF(p.Value))
			}
		}
	}
}

// JSON — Prometheus-compatible format
type JSON struct{}

func (f *JSON) Format(w io.Writer, r *types.QueryResult) {
	type result struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	resp := result{Status: "success"}
	resp.Data.ResultType = r.Type

	switch r.Type {
	case "matrix":
		type mResult struct {
			Metric map[string]string `json:"metric"`
			Values [][]any           `json:"values"`
		}
		var mrs []mResult
		for _, s := range r.Matrix {
			mr := mResult{Metric: s.Labels}
			for _, p := range s.Samples {
				mr.Values = append(mr.Values, []any{
					float64(p.Timestamp) / 1000.0, fmtF(p.Value),
				})
			}
			mrs = append(mrs, mr)
		}
		resp.Data.Result, _ = json.Marshal(mrs)
	case "vector":
		type vResult struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
		}
		var vrs []vResult
		for _, s := range r.Vector {
			vrs = append(vrs, vResult{
				Metric: s.Labels,
				Value:  []any{float64(s.T) / 1000.0, fmtF(s.F)},
			})
		}
		resp.Data.Result, _ = json.Marshal(vrs)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(resp)
}

// CSV
type CSV struct{}

func (f *CSV) Format(w io.Writer, r *types.QueryResult) {
	cw := csv.NewWriter(w)
	defer cw.Flush()
	switch r.Type {
	case "vector":
		cw.Write([]string{"labels", "timestamp", "value"})
		for _, s := range r.Vector {
			cw.Write([]string{labelsStr(s.Labels), fmtTs(s.T), fmtF(s.F)})
		}
	case "matrix":
		cw.Write([]string{"labels", "timestamp", "value"})
		for _, s := range r.Matrix {
			lbl := labelsStr(s.Labels)
			for _, p := range s.Samples {
				cw.Write([]string{lbl, fmtTs(p.Timestamp), fmtF(p.Value)})
			}
		}
	}
}

// SQL — prints SQL to stdout (used with --sql-only)
type SQL struct{}

func (f *SQL) Format(w io.Writer, r *types.QueryResult) {
	fmt.Fprintln(w, "-- use --sql-only for SQL output")
}

func labelsStr(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	var b strings.Builder
	b.WriteByte('{')
	keys := sortedKeys(m)
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `%s="%s"`, k, m[k])
	}
	b.WriteByte('}')
	return b.String()
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func fmtTs(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

func fmtF(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
