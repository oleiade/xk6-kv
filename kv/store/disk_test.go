//go:build !windows

package store

import (
	"path/filepath"
	"testing"
)

// Cannot run in parallel: t.Chdir mutates process-wide working directory.
// The empty-path fallback opens DefaultDiskStorePath in cwd, so we point
// cwd at a tempdir to keep the test hermetic.
//
//nolint:paralleltest
func TestNewDiskStore_EmptyPathUsesDefault(t *testing.T) {
	t.Chdir(t.TempDir())

	s, err := NewDiskStore("")
	if err != nil {
		t.Fatalf("NewDiskStore(\"\"): %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if s.path != DefaultDiskStorePath {
		t.Fatalf("NewDiskStore(\"\") path = %s, want %s", s.path, DefaultDiskStorePath)
	}
}

func TestNewDiskStore_HonorsExplicitPath(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "custom.db")
	s, err := NewDiskStore(path)
	if err != nil {
		t.Fatalf("NewDiskStore(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if s.path != path {
		t.Fatalf("NewDiskStore(%q) path = %s", path, s.path)
	}
}

// TestDiskStore_Contract verifies the on-disk backend against the shared
// Backend contract. See store_contract_test.go for the cases covered.
func TestDiskStore_Contract(t *testing.T) {
	t.Parallel()
	runBackendContract(t, newDiskStoreForTest)
}

// TestDiskStore_PersistsAcrossReopen confirms data written through one
// DiskStore handle is visible after the file is closed and reopened.
func TestDiskStore_PersistsAcrossReopen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.db")

	writer, err := NewDiskStore(path)
	if err != nil {
		t.Fatalf("NewDiskStore: %v", err)
	}
	if err := writer.Set("k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := NewDiskStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
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
	s, err := NewDiskStore(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("NewDiskStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
