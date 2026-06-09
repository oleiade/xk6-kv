package kv

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestBuildStore_Defaults exercises buildStore with the package defaults,
// which include the disk backend. The default path is relative, so we point
// cwd at a tempdir to keep the test hermetic — that rules out t.Parallel.
// Skipped on Windows for the same reason disk_test.go is build-gated there:
// BoltDB's file-lock semantics on Windows can prevent t.TempDir from cleaning
// up the lock file even after Close.
//
//nolint:paralleltest
func TestBuildStore_Defaults(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("disk backend tests are skipped on Windows; see disk_test.go")
	}
	t.Chdir(t.TempDir())

	s, err := buildStore(Options{Backend: DefaultBackend, Serialization: DefaultSerialization})
	if err != nil {
		t.Fatalf("buildStore() returned an error: %v", err)
	}
	if s == nil {
		t.Fatal("buildStore() returned a nil store")
	}
	t.Cleanup(func() { _ = s.Close() })
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

// TestRootModule_ConcurrentInitIsRaceFree drives the lazy store init from
// many goroutines via acquireStore (the path OpenKv uses). It verifies two
// things at once: every caller sees the same shared Store pointer, and only
// one of them actually constructs the store. Run with -race for the
// race-detector signal.
func TestRootModule_ConcurrentInitIsRaceFree(t *testing.T) {
	t.Parallel()

	const goroutines = 32
	opts := Options{Backend: "memory", Serialization: "json"}

	rm := &RootModule{}

	// Wrap the real buildStore call site by intercepting through
	// acquireStore — we can't inject a factory cheaply, so we instead
	// count how many times acquireStore actually built a store by
	// comparing pointer identity afterwards.
	var (
		wg      sync.WaitGroup
		errs    atomic.Int64
		stores  = make([]any, goroutines)
		storesM sync.Mutex
	)

	wg.Add(goroutines)
	for i := range goroutines {
		go func() {
			defer wg.Done()
			s, _, err := rm.acquireStore(opts)
			if err != nil {
				errs.Add(1)
				return
			}
			storesM.Lock()
			stores[i] = s
			storesM.Unlock()
		}()
	}
	wg.Wait()

	if got := errs.Load(); got != 0 {
		t.Fatalf("%d acquireStore calls returned an error", got)
	}
	if rm.store == nil {
		t.Fatal("store remains nil after concurrent init")
	}

	// Every successful caller must see the same pointer — proves
	// acquireStore built the store exactly once and didn't race a
	// later caller into rebuilding.
	first := stores[0]
	if first == nil {
		t.Fatal("first goroutine got a nil store")
	}
	for i, s := range stores {
		if s != first {
			t.Fatalf("goroutine %d saw a different store pointer than goroutine 0", i)
		}
	}
}

// TestRootModule_InitErrorIsNotLatched verifies that a failed first
// acquireStore call does NOT permanently break the module — a subsequent
// call with valid options must be free to retry construction.
func TestRootModule_InitErrorIsNotLatched(t *testing.T) {
	t.Parallel()

	rm := &RootModule{}

	// "bogus" is not a registered backend; buildStore must fail.
	_, _, err := rm.acquireStore(Options{Backend: "bogus", Serialization: "json"})
	if err == nil {
		t.Fatal("expected acquireStore to fail with unknown backend, got nil error")
	}
	if rm.store != nil {
		t.Fatal("failed acquireStore must not populate rm.store")
	}

	// Now a valid call must succeed — the prior failure must not have
	// latched the module into a permanent error state.
	s, _, err := rm.acquireStore(Options{Backend: "memory", Serialization: "json"})
	if err != nil {
		t.Fatalf("retry after failed init returned an error: %v", err)
	}
	if s == nil {
		t.Fatal("retry returned a nil store")
	}
}
