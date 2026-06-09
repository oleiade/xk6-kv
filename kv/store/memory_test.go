package store

import (
	"testing"
)

func TestNewMemoryStore(t *testing.T) {
	t.Parallel()

	s := NewMemoryStore()
	if s == nil {
		t.Fatal("NewMemoryStore() returned nil")
	}
	if s.container == nil {
		t.Fatal("NewMemoryStore() returned a store with a nil container")
	}
	if len(s.container) != 0 {
		t.Fatalf("NewMemoryStore() container has %d entries, want 0", len(s.container))
	}
}

// TestMemoryStore_Contract verifies the in-memory backend against the shared
// Backend contract. See store_contract_test.go for the cases covered.
func TestMemoryStore_Contract(t *testing.T) {
	t.Parallel()
	runBackendContract(t, func(_ *testing.T) Backend {
		return NewMemoryStore()
	})
}

// TestMemoryStore_Concurrency stresses the RWMutex protection by overlapping
// reads and writes. The test asserts only that the run completes without
// deadlocking or tripping the race detector (run with -race).
func TestMemoryStore_Concurrency(t *testing.T) {
	t.Parallel()

	s := NewMemoryStore()
	done := make(chan struct{})

	go func() {
		for range 100 {
			_ = s.Set("key", []byte("value"))
		}
		done <- struct{}{}
	}()
	go func() {
		for range 100 {
			_, _ = s.Get("key")
		}
		done <- struct{}{}
	}()

	<-done
	<-done
}
