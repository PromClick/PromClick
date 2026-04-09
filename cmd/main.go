package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/PromClick/PromClick/clickhouse"
	"github.com/PromClick/PromClick/config"
	"github.com/PromClick/PromClick/eval"
	"github.com/PromClick/PromClick/output"
	"github.com/PromClick/PromClick/translator"
)

func main() {
	query := flag.String("query", "", "PromQL query")
	startS := flag.String("start", "now-1h", "Start time (RFC3339, -1h, -30m, 'now')")
	endS := flag.String("end", "now", "End time")
	stepS := flag.String("step", "60s", "Evaluation step")
	cfgPath := flag.String("config", "promql2chsql.yaml", "Config file")
	format := flag.String("format", "table", "Output: table|json|csv|sql")
	sqlOnly := flag.Bool("sql-only", false, "Only print SQL")
	explain := flag.Bool("explain", false, "Print AST to stderr")
	timeout := flag.Duration("timeout", 30*time.Second, "Query timeout")
	flag.StringVar(query, "q", "", "")
	flag.StringVar(cfgPath, "c", "promql2chsql.yaml", "")
	flag.StringVar(format, "f", "table", "")
	flag.Parse()

	// Query: flag, positional arg, or stdin
	q := *query
	if q == "" && flag.NArg() > 0 {
		q = strings.Join(flag.Args(), " ")
	}
	if q == "" {
		q = readStdin()
	}
	if q == "" {
		fatalf("no query provided (-q, positional arg, or stdin)")
	}

	now := time.Now()
	start, err := parseRelTime(*startS, now)
	if err != nil {
		fatalf("--start: %v", err)
	}
	end, err := parseRelTime(*endS, now)
	if err != nil {
		fatalf("--end: %v", err)
	}
	step, err := time.ParseDuration(*stepS)
	if err != nil {
		fatalf("--step: %v", err)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fatalf("config: %v", err)
	}

	// Transpilation
	tr := translator.New(cfg, start, end, step)
	plan, err := tr.TranspileQuery(q)
	if err != nil {
		fatalf("parse/translate: %v", err)
	}

	if *explain {
		fmt.Fprintf(os.Stderr, "AST:\n%s\n", plan.AST)
	}

	sql, _ := plan.Render()
	if cfg.Output.ShowSQL || *sqlOnly {
		fmt.Fprintf(os.Stderr, "-- SQL:\n%s\n", sql)
	}
	if *sqlOnly {
		fmt.Println(sql)
		return
	}

	// Execution + Evaluation
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client := clickhouse.NewClient(cfg.ClickHouse)
	evaluator := eval.NewWithClient(cfg, client)
	result, err := evaluator.EvalPlan(ctx, plan, start, end, step)
	if err != nil {
		fatalf("eval: %v", err)
	}

	// Formatting
	f, err := output.NewFormatter(*format)
	if err != nil {
		fatalf("format: %v", err)
	}
	f.Format(os.Stdout, result)
}

func readStdin() string {
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) != 0 {
		return ""
	}
	var lines []string
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l != "" && !strings.HasPrefix(l, "#") {
			lines = append(lines, l)
		}
	}
	return strings.Join(lines, " ")
}

func parseRelTime(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "now" {
		return now, nil
	}

	neg := false
	rel := ""
	if strings.HasPrefix(s, "now-") {
		neg = true
		rel = s[4:]
	} else if strings.HasPrefix(s, "now+") {
		rel = s[4:]
	} else if strings.HasPrefix(s, "-") {
		neg = true
		rel = s[1:]
	} else if strings.HasPrefix(s, "+") {
		rel = s[1:]
	}

	if rel != "" {
		d, err := time.ParseDuration(rel)
		if err != nil {
			return time.Time{}, err
		}
		if neg {
			return now.Add(-d), nil
		}
		return now.Add(d), nil
	}

	// RFC3339 and variants
	for _, layout := range []string{
		time.RFC3339, time.RFC3339Nano,
		"2006-01-02T15:04:05", "2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse time: %q", s)
}

func fatalf(f string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+f+"\n", args...)
	os.Exit(1)
}
