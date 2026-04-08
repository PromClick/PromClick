package promql2chsql

import "fmt"

type ParseError struct {
	Query   string
	Message string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("PromQL parse error: %s\n  query: %s", e.Message, e.Query)
}

type UnsupportedError struct {
	Feature string
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("unsupported: %s", e.Feature)
}

type CHError struct {
	StatusCode int
	Message    string
}

func (e *CHError) Error() string {
	return fmt.Sprintf("ClickHouse HTTP %d: %s", e.StatusCode, e.Message)
}

type TooManySeriesError struct {
	Count, Max int
}

func (e *TooManySeriesError) Error() string {
	return fmt.Sprintf("too many series: %d (max %d)", e.Count, e.Max)
}
