package types

// Sample is a raw sample from ClickHouse.
// Timestamp in milliseconds (unix ms).
type Sample struct {
	Timestamp int64
	Value     float64
}

// Series is a full series with labels and samples.
type Series struct {
	Labels      map[string]string
	Fingerprint uint64
	Samples     []Sample // sorted by Timestamp ASC
}

// InstantSample is the result of an instant query for a single series.
type InstantSample struct {
	Labels      map[string]string
	Fingerprint uint64
	T           int64   // eval_time w ms
	F           float64 // value
}

// Vector = []InstantSample (instant query result)
type Vector []InstantSample

// Matrix = []Series (range query result)
type Matrix []Series

// QueryResult wraps a result with its type.
type QueryResult struct {
	Type   string // "matrix" | "vector" | "scalar"
	Matrix Matrix
	Vector Vector
}

func (q *QueryResult) IsEmpty() bool {
	return len(q.Matrix) == 0 && len(q.Vector) == 0
}
