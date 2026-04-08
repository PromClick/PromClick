package types

// Sample to surowa próbka z ClickHouse.
// Timestamp w milisekundach (unix ms).
type Sample struct {
	Timestamp int64
	Value     float64
}

// Series to pełna seria z labelami i próbkami.
type Series struct {
	Labels      map[string]string
	Fingerprint uint64
	Samples     []Sample // posortowane po Timestamp ASC
}

// InstantSample to wynik instant query dla jednej serii.
type InstantSample struct {
	Labels      map[string]string
	Fingerprint uint64
	T           int64   // eval_time w ms
	F           float64 // wartość
}

// Vector = []InstantSample (instant query result)
type Vector []InstantSample

// Matrix = []Series (range query result)
type Matrix []Series

// QueryResult opakowuje wynik z typem.
type QueryResult struct {
	Type   string // "matrix" | "vector" | "scalar"
	Matrix Matrix
	Vector Vector
}

func (q *QueryResult) IsEmpty() bool {
	return len(q.Matrix) == 0 && len(q.Vector) == 0
}
