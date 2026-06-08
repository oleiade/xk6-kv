package store

import (
	"fmt"
	"testing"
)

// backendBenchFactory produces a fresh, empty Backend for each benchmark.
type backendBenchFactory func(b *testing.B) Backend

// runBackendBenchmarks exercises the common Backend hot paths against a
// concrete implementation. Sub-benchmarks: Get, Set, Delete, Exists, List
// (with prefix/limit variants), Concurrent.
func runBackendBenchmarks(b *testing.B, factory backendBenchFactory) {
	b.Helper()

	b.Run("Get", func(b *testing.B) {
		backend := factory(b)
		seed(b, backend, 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = backend.Get(keyAt(i % 1000))
		}
	})

	b.Run("Set", func(b *testing.B) {
		backend := factory(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = backend.Set(keyAt(i), []byte(valueAt(i)))
		}
	})

	b.Run("Delete", func(b *testing.B) {
		for _, size := range []int{10, 100, 1000} {
			b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
				backend := factory(b)
				seed(b, backend, size)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					k := keyAt(i % size)
					_ = backend.Delete(k)
					if i < b.N-1 {
						b.StopTimer()
						_ = backend.Set(k, []byte(valueAt(i%size)))
						b.StartTimer()
					}
				}
			})
		}
	})

	b.Run("Exists", func(b *testing.B) {
		backend := factory(b)
		seed(b, backend, 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = backend.Exists(keyAt(i % 1000))
		}
	})

	b.Run("List", func(b *testing.B) {
		backend := factory(b)
		seed(b, backend, 1000)
		for i := 0; i < 100; i++ {
			_ = backend.Set(fmt.Sprintf("prefix-%d", i), []byte(valueAt(i)))
		}

		variants := []struct {
			name   string
			prefix string
			limit  int64
		}{
			{"All", "", 0},
			{"WithPrefix", "prefix", 0},
			{"WithLimit", "", 10},
			{"WithPrefixAndLimit", "prefix", 10},
		}
		for _, v := range variants {
			b.Run(v.name, func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = backend.List(v.prefix, v.limit)
				}
			})
		}
	})

	b.Run("Concurrent", func(b *testing.B) {
		backend := factory(b)
		seed(b, backend, 1000)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				k := keyAt(i % 1000)
				if i%2 == 0 {
					_, _ = backend.Get(k)
				} else {
					_ = backend.Set(k, []byte(valueAt(i)))
				}
				i++
			}
		})
	})
}

func seed(b *testing.B, backend Backend, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		if err := backend.Set(keyAt(i), []byte(valueAt(i))); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
}

func keyAt(i int) string   { return fmt.Sprintf("key-%d", i) }
func valueAt(i int) string { return fmt.Sprintf("value-%d", i) }
