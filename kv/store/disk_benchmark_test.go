//nolint:forbidigo,errcheck,gosec
package store

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkDiskStore_Get(b *testing.B) {
	run := func(b *testing.B, trackKeys bool) {
		// Create a temporary file for testing
		tempFile, err := os.CreateTemp(b.TempDir(), "diskstore-bench-*.db")
		if err != nil {
			b.Fatalf("Failed to create temporary file: %v", err)
		}

		tempFile.Close()

		defer os.Remove(tempFile.Name())

		store := NewDiskStore(trackKeys)
		store.path = tempFile.Name()

		// Setup: Add some data to the store
		for i := range 1000 {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			err := store.Set(key, value)
			if err != nil {
				b.Fatalf("Failed to set up benchmark: %v", err)
			}
		}

		// Reset the timer before the actual benchmark
		b.ResetTimer()

		// Run the benchmark
		for i := range b.N {
			key := fmt.Sprintf("key-%d", i%1000)
			_, _ = store.Get(key)
		}

		// Clean up
		b.StopTimer()
		_ = store.Close()
	}

	b.Run("WithTrackKeys", func(b *testing.B) {
		run(b, true)
	})

	b.Run("WithoutTrackKeys", func(b *testing.B) {
		run(b, false)
	})
}

func BenchmarkDiskStore_Set(b *testing.B) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp(b.TempDir(), "diskstore-bench-*.db")
	if err != nil {
		b.Fatalf("Failed to create temporary file: %v", err)
	}

	tempFile.Close()

	defer os.Remove(tempFile.Name())

	store := NewDiskStore(true)
	store.path = tempFile.Name()

	// Reset the timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := range b.N {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		_ = store.Set(key, value)
	}

	// Clean up
	b.StopTimer()
	_ = store.Close()
}

func BenchmarkDiskStore_Delete(b *testing.B) {
	// Run the benchmark with a smaller size due to disk I/O constraints
	benchSizes := []int{10, 100, 1000}

	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			// Create a temporary file for testing
			tempFile, err := os.CreateTemp(b.TempDir(), "diskstore-bench-*.db")
			if err != nil {
				b.Fatalf("Failed to create temporary file: %v", err)
			}

			tempFile.Close()

			defer os.Remove(tempFile.Name())

			store := NewDiskStore(true)
			store.path = tempFile.Name()

			// Setup: Add data to the store
			for i := range size {
				key := fmt.Sprintf("key-%d", i)
				value := fmt.Sprintf("value-%d", i)

				err := store.Set(key, value)
				if err != nil {
					b.Fatalf("Failed to set up benchmark: %v", err)
				}
			}

			// Reset the timer before the actual benchmark
			b.ResetTimer()

			// Run the benchmark
			for i := range b.N {
				key := fmt.Sprintf("key-%d", i%size)
				_ = store.Delete(key)

				// Re-add the key for the next iteration
				if i < b.N-1 {
					b.StopTimer()
					value := fmt.Sprintf("value-%d", i%size)
					_ = store.Set(key, value)
					b.StartTimer()
				}
			}

			// Clean up
			b.StopTimer()
			_ = store.Close()
		})
	}
}

func BenchmarkDiskStore_Exists(b *testing.B) {
	run := func(b *testing.B, trackKeys bool) {
		// Create a temporary file for testing
		tempFile, err := os.CreateTemp(b.TempDir(), "diskstore-bench-*.db")
		if err != nil {
			b.Fatalf("Failed to create temporary file: %v", err)
		}

		tempFile.Close()

		defer os.Remove(tempFile.Name())

		store := NewDiskStore(trackKeys)
		store.path = tempFile.Name()

		// Setup: Add some data to the store
		for i := range 1000 {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)

			err := store.Set(key, value)
			if err != nil {
				b.Fatalf("Failed to set up benchmark: %v", err)
			}
		}

		// Reset the timer before the actual benchmark
		b.ResetTimer()

		// Run the benchmark
		for i := range b.N {
			key := fmt.Sprintf("key-%d", i%1000)
			_, _ = store.Exists(key)
		}

		// Clean up
		b.StopTimer()
		_ = store.Close()
	}

	b.Run("WithTrackKeys", func(b *testing.B) {
		run(b, true)
	})

	b.Run("WithoutTrackKeys", func(b *testing.B) {
		run(b, false)
	})
}

func BenchmarkDiskStore_List(b *testing.B) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp(b.TempDir(), "diskstore-bench-*.db")
	if err != nil {
		b.Fatalf("Failed to create temporary file: %v", err)
	}

	tempFile.Close()

	defer os.Remove(tempFile.Name())

	store := NewDiskStore(true)
	store.path = tempFile.Name()

	// Setup: Add some data to the store
	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)

		err := store.Set(key, value)
		if err != nil {
			b.Fatalf("Failed to set up benchmark: %v", err)
		}
	}

	// Add some data with a specific prefix
	for i := range 100 {
		key := fmt.Sprintf("prefix-%d", i)
		value := fmt.Sprintf("value-%d", i)

		err := store.Set(key, value)
		if err != nil {
			b.Fatalf("Failed to set up benchmark: %v", err)
		}
	}

	// Benchmark different List operations
	b.Run("ListAll", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = store.List("", 0)
		}
	})

	b.Run("ListWithPrefix", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = store.List("prefix", 0)
		}
	})

	b.Run("ListWithLimit", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = store.List("", 10)
		}
	})

	b.Run("ListWithPrefixAndLimit", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = store.List("prefix", 10)
		}
	})

	// Clean up
	b.StopTimer()
	_ = store.Close()
}

func BenchmarkDiskStore_Concurrent(b *testing.B) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp(b.TempDir(), "diskstore-bench-*.db")
	if err != nil {
		b.Fatalf("Failed to create temporary file: %v", err)
	}

	tempFile.Close()

	defer os.Remove(tempFile.Name())

	store := NewDiskStore(true)
	store.path = tempFile.Name()

	// Setup: Add some data to the store
	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)

		err := store.Set(key, value)
		if err != nil {
			b.Fatalf("Failed to set up benchmark: %v", err)
		}
	}

	// Reset the timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark with multiple goroutines
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Alternate between Get and Set operations
			if i%2 == 0 {
				key := fmt.Sprintf("key-%d", i%1000)
				_, _ = store.Get(key)
			} else {
				key := fmt.Sprintf("key-%d", i%1000)
				value := fmt.Sprintf("value-%d", i)
				_ = store.Set(key, value)
			}

			i++
		}
	})

	// Clean up
	b.StopTimer()
	_ = store.Close()
}

func BenchmarkDiskStore_RandomKey(b *testing.B) {
	run := func(b *testing.B, trackKeys bool) {
		b.Helper()

		tempFile, err := os.CreateTemp(b.TempDir(), "randomkey-bench-*.db")
		if err != nil {
			b.Fatalf("Failed to create temporary file: %v", err)
		}

		defer os.Remove(tempFile.Name())

		store := NewDiskStore(trackKeys)
		store.path = tempFile.Name()

		// Setup data
		for i := range 10_000 {
			_ = store.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
		}

		b.ResetTimer()

		for range b.N {
			_, _ = store.RandomKey()
		}

		b.StopTimer()
		_ = store.Close()
	}

	b.Run("WithTrackKeys", func(b *testing.B) {
		run(b, true)
	})

	b.Run("WithoutTrackKeys", func(b *testing.B) {
		run(b, false)
	})
}

func BenchmarkDiskStore_RebuildKeyList(b *testing.B) {
	run := func(b *testing.B, trackKeys bool) {
		b.Helper()

		tempFile, err := os.CreateTemp(b.TempDir(), "rebuildkeylist-bench-*.db")
		if err != nil {
			b.Fatalf("Failed to create temporary file: %v", err)
		}

		defer os.Remove(tempFile.Name())

		store := NewDiskStore(trackKeys)
		store.path = tempFile.Name()

		// Fill with keys
		for i := range 10_000 {
			_ = store.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
		}

		b.ResetTimer()

		for range b.N {
			_ = store.RebuildKeyList()
		}

		b.StopTimer()
		_ = store.Close()
	}

	b.Run("WithTrackKeys", func(b *testing.B) {
		run(b, true)
	})

	b.Run("WithoutTrackKeys", func(b *testing.B) {
		run(b, false)
	})
}
