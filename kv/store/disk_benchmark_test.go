//nolint:forbidigo,errcheck,gosec
package store

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkDiskStore_Get(b *testing.B) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp(b.TempDir(), "diskstore-bench-*.db")
	if err != nil {
		b.Fatalf("Failed to create temporary file: %v", err)
	}
	tempFile.Close()
	defer os.Remove(tempFile.Name())

	store := NewDiskStore()
	store.path = tempFile.Name()

	// Setup: Add some data to the store
	for i := 0; i < 1000; i++ {
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
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%1000)
		_, _ = store.Get(key)
	}

	// Clean up
	b.StopTimer()
	_ = store.Close()
}

func BenchmarkDiskStore_Set(b *testing.B) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp(b.TempDir(), "diskstore-bench-*.db")
	if err != nil {
		b.Fatalf("Failed to create temporary file: %v", err)
	}
	tempFile.Close()
	defer os.Remove(tempFile.Name())

	store := NewDiskStore()
	store.path = tempFile.Name()

	// Reset the timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
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

			store := NewDiskStore()
			store.path = tempFile.Name()

			// Setup: Add data to the store
			for i := 0; i < size; i++ {
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
			for i := 0; i < b.N; i++ {
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
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp(b.TempDir(), "diskstore-bench-*.db")
	if err != nil {
		b.Fatalf("Failed to create temporary file: %v", err)
	}
	tempFile.Close()
	defer os.Remove(tempFile.Name())

	store := NewDiskStore()
	store.path = tempFile.Name()

	// Setup: Add some data to the store
	for i := 0; i < 1000; i++ {
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
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%1000)
		_, _ = store.Exists(key)
	}

	// Clean up
	b.StopTimer()
	_ = store.Close()
}

func BenchmarkDiskStore_List(b *testing.B) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp(b.TempDir(), "diskstore-bench-*.db")
	if err != nil {
		b.Fatalf("Failed to create temporary file: %v", err)
	}
	tempFile.Close()
	defer os.Remove(tempFile.Name())

	store := NewDiskStore()
	store.path = tempFile.Name()

	// Setup: Add some data to the store
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := store.Set(key, value)
		if err != nil {
			b.Fatalf("Failed to set up benchmark: %v", err)
		}
	}

	// Add some data with a specific prefix
	for i := 0; i < 100; i++ {
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
		for i := 0; i < b.N; i++ {
			_, _ = store.List("", 0)
		}
	})

	b.Run("ListWithPrefix", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = store.List("prefix", 0)
		}
	})

	b.Run("ListWithLimit", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = store.List("", 10)
		}
	})

	b.Run("ListWithPrefixAndLimit", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
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

	store := NewDiskStore()
	store.path = tempFile.Name()

	// Setup: Add some data to the store
	for i := 0; i < 1000; i++ {
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
