package kv

import (
	"fmt"
	"strings"
	"testing"

	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/modulestest"
)

// optsValue builds a sobek-wrapped options object inside rt. OpenKv reads
// options via the calling VU's runtime, so the value must live in the same
// runtime as the caller.
func optsValue(rt *sobek.Runtime, backend string) sobek.Value {
	return rt.ToValue(map[string]string{
		"backend":       backend,
		"serialization": "json",
	})
}

// catchThrow runs fn and recovers any panic raised by common.Throw, returning
// a string describing it. Returns "" if fn did not panic.
func catchThrow(fn func()) string {
	var caught string
	func() {
		defer func() {
			if r := recover(); r != nil {
				caught = fmt.Sprintf("%v", r)
			}
		}()
		fn()
	}()
	return caught
}

// TestOpenKv_SharesStoreAcrossVUs verifies the load-bearing multi-VU property
// of the module: every VU that calls openKv() with matching options sees the
// same shared Store, so writes from one VU are visible to every other.
func TestOpenKv_SharesStoreAcrossVUs(t *testing.T) {
	t.Parallel()

	rm := New()
	rtA := modulestest.NewRuntime(t)
	rtB := modulestest.NewRuntime(t)

	miA, _ := rm.NewModuleInstance(rtA.VU).(*ModuleInstance)
	miB, _ := rm.NewModuleInstance(rtB.VU).(*ModuleInstance)

	if obj := miA.OpenKv(optsValue(rtA.VU.Runtime(), "memory")); obj == nil {
		t.Fatal("first OpenKv returned nil")
	}
	if obj := miB.OpenKv(optsValue(rtB.VU.Runtime(), "memory")); obj == nil {
		t.Fatal("second OpenKv returned nil")
	}

	// Both ModuleInstances must reference the same shared store pointer.
	if miA.rm.store != miB.rm.store {
		t.Fatal("VUs do not share the same store")
	}

	// Cross-VU visibility: write via A's view, read via B's view.
	if err := miA.rm.store.Set("k", "v"); err != nil {
		t.Fatalf("Set via A: %v", err)
	}
	got, err := miB.rm.store.Get("k")
	if err != nil {
		t.Fatalf("Get via B: %v", err)
	}
	if got != "v" {
		t.Fatalf("Get via B = %v, want %q", got, "v")
	}
}

// TestOpenKv_MismatchedOptionsErrors covers the storeOpts != options branch:
// once the shared store is opened with one backend, a later VU asking for a
// different backend must error out — VUs cannot reopen the store with
// different settings.
func TestOpenKv_MismatchedOptionsErrors(t *testing.T) {
	t.Parallel()

	rm := New()
	rtA := modulestest.NewRuntime(t)
	rtB := modulestest.NewRuntime(t)

	miA, _ := rm.NewModuleInstance(rtA.VU).(*ModuleInstance)
	miB, _ := rm.NewModuleInstance(rtB.VU).(*ModuleInstance)

	if obj := miA.OpenKv(optsValue(rtA.VU.Runtime(), "memory")); obj == nil {
		t.Fatal("first OpenKv returned nil")
	}

	thrown := catchThrow(func() {
		_ = miB.OpenKv(optsValue(rtB.VU.Runtime(), "disk"))
	})
	if thrown == "" {
		t.Fatal("expected mismatched-options OpenKv to throw, got no panic")
	}
	if !strings.Contains(thrown, "already initialized") {
		t.Fatalf("thrown error = %q, want it to mention 'already initialized'", thrown)
	}
}
