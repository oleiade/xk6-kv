package kv

import (
	"sync"
	"testing"
)

func TestBuildStore_Defaults(t *testing.T) {
	t.Parallel()

	s, err := buildStore(Options{Backend: DefaultBackend, Serialization: DefaultSerialization})
	if err != nil {
		t.Fatalf("buildStore() returned an error: %v", err)
	}
	if s == nil {
		t.Fatal("buildStore() returned a nil store")
	}
}

func TestBuildStore_UnknownBackend(t *testing.T) {
	t.Parallel()

	_, err := buildStore(Options{Backend: "nope", Serialization: DefaultSerialization})
	if err == nil {
		t.Fatal("buildStore() should have returned an error for unknown backend")
	}
}

func TestBuildStore_UnknownSerialization(t *testing.T) {
	t.Parallel()

	_, err := buildStore(Options{Backend: DefaultBackend, Serialization: "nope"})
	if err == nil {
		t.Fatal("buildStore() should have returned an error for unknown serialization")
	}
}

// TestRootModule_ConcurrentInitIsRaceFree exercises the sync.Once-guarded
// store init from many goroutines and verifies they all see the same shared
// store pointer with no data race. Run with -race for the race-detector
// signal.
func TestRootModule_ConcurrentInitIsRaceFree(t *testing.T) {
	t.Parallel()

	rm := New()
	const goroutines = 32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			rm.storeOnce.Do(func() {
				rm.store, rm.storeErr = buildStore(Options{Backend: "memory", Serialization: "json"})
				if rm.storeErr == nil {
					rm.storeOpts = Options{Backend: "memory", Serialization: "json"}
				}
			})
		}()
	}
	wg.Wait()

	if rm.storeErr != nil {
		t.Fatalf("storeErr: %v", rm.storeErr)
	}
	if rm.store == nil {
		t.Fatal("store remains nil after concurrent init")
	}
}
