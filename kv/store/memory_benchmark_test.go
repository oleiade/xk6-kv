package store

import "testing"

func BenchmarkMemoryStore(b *testing.B) {
	runBackendBenchmarks(b, func(_ *testing.B) Backend {
		return NewMemoryStore()
	})
}
