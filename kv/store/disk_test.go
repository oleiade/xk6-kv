//go:build !windows

package store

import (
	"path/filepath"
	"testing"
)

func TestNewDiskStore(t *testing.T) {
	t.Parallel()

	// Empty path falls back to the default location.
	s := NewDiskStore("")
	if s == nil {
		t.Fatal("NewDiskStore() returned nil")
	}
	if s.path != DefaultDiskStorePath {
		t.Fatalf("NewDiskStore(\"\") path = %s, want %s", s.path, DefaultDiskStorePath)
	}

	// Explicit path is honored.
	custom := NewDiskStore("/tmp/custom.db")
	if custom.path != "/tmp/custom.db" {
		t.Fatalf("NewDiskStore(/tmp/custom.db) path = %s", custom.path)
	}

	if s.handle == nil {
		t.Fatal("NewDiskStore() handle is nil")
	}
	if s.opened.Load() {
		t.Fatal("NewDiskStore() is already marked open")
	}
	if s.refCount.Load() != 0 {
		t.Fatalf("NewDiskStore() refCount = %d, want 0", s.refCount.Load())
	}
}

// TestDiskStore_Contract verifies the on-disk backend against the shared
// Backend contract. See store_contract_test.go for the cases covered.
func TestDiskStore_Contract(t *testing.T) {
	t.Parallel()
	runBackendContract(t, newDiskStoreForTest)
}

// TestDiskStore_RefCountedClose checks the reference-counted Close semantics
// that are specific to the disk backend: each operation that triggers an
// implicit open() increments the refcount; Close decrements it and only
// closes the underlying BoltDB when the count reaches zero.
func TestDiskStore_RefCountedClose(t *testing.T) {
	t.Parallel()

	s := newDiskStoreForTest(t).(*DiskStore)

	if err := s.Set("k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := s.refCount.Load(); got != 1 {
		t.Fatalf("refCount after Set = %d, want 1", got)
	}

	if _, err := s.Get("k"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := s.refCount.Load(); got != 2 {
		t.Fatalf("refCount after Get = %d, want 2", got)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := s.refCount.Load(); got != 1 {
		t.Fatalf("refCount after first Close = %d, want 1", got)
	}
	if !s.opened.Load() {
		t.Fatal("store should still be open after first Close")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := s.refCount.Load(); got != 0 {
		t.Fatalf("refCount after second Close = %d, want 0", got)
	}
	if s.opened.Load() {
		t.Fatal("store should be closed after refcount reaches zero")
	}
}

// TestDiskStore_PersistsAcrossReopen confirms data written through one
// DiskStore handle is visible after the file is closed and reopened.
func TestDiskStore_PersistsAcrossReopen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.db")

	writer := NewDiskStore(path)
	if err := writer.Set("k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader := NewDiskStore(path)
	t.Cleanup(func() { _ = reader.Close() })

	got, err := reader.Get("k")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("reopened Get returned %q, want %q", got, "v")
	}
}

func newDiskStoreForTest(t *testing.T) Backend {
	t.Helper()
	s := NewDiskStore(filepath.Join(t.TempDir(), "store.db"))
	t.Cleanup(func() { _ = s.Close() })
	return s
}
