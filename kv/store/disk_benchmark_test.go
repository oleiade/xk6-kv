//go:build !windows

package store

import (
	"path/filepath"
	"testing"
)

func BenchmarkDiskStore(b *testing.B) {
	runBackendBenchmarks(b, func(b *testing.B) Backend {
		s, err := NewDiskStore(filepath.Join(b.TempDir(), "store.db"))
		if err != nil {
			b.Fatalf("NewDiskStore: %v", err)
		}
		b.Cleanup(func() { _ = s.Close() })
		return s
	})
}
