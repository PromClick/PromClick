package eval

import (
	"math"
	"testing"
	"time"

	"github.com/hinskii/promclick/types"
)

func generateSamples(n int, duration time.Duration) []types.Sample {
	samples := make([]types.Sample, n)
	start := time.Now().Add(-duration).UnixMilli()
	step := duration.Milliseconds() / int64(n)
	for i := 0; i < n; i++ {
		samples[i] = types.Sample{
			Timestamp: start + int64(i)*step,
			Value:     float64(i) * 1.5,
		}
	}
	return samples
}

func BenchmarkWindowSamples(b *testing.B) {
	samples := generateSamples(1000, time.Hour)
	start := samples[400].Timestamp
	end := samples[600].Timestamp
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WindowSamples(samples, start, end)
	}
}

func BenchmarkExtrapolatedRate(b *testing.B) {
	samples := generateSamples(20, 5*time.Minute)
	start := samples[0].Timestamp
	end := samples[len(samples)-1].Timestamp
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtrapolatedRate(samples, start, end, true, true)
	}
}

func BenchmarkInstantValue(b *testing.B) {
	samples := generateSamples(1000, time.Hour)
	evalTime := samples[500].Timestamp
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		InstantValue(samples, evalTime, 300_000)
	}
}

func BenchmarkHistogramQuantile(b *testing.B) {
	buckets := []Bucket{
		{0.005, 10}, {0.01, 20}, {0.025, 50}, {0.05, 80},
		{0.1, 150}, {0.25, 300}, {0.5, 500}, {1.0, 800},
		{2.5, 950}, {5.0, 990}, {10.0, 999}, {math.Inf(1), 1000},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HistogramQuantile(0.99, append([]Bucket{}, buckets...))
	}
}
