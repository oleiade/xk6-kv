//go:build !windows

package store

import (
	"path/filepath"
	"testing"
)

func BenchmarkDiskStore(b *testing.B) {
	runBackendBenchmarks(b, func(b *testing.B) Backend {
		s := NewDiskStore(filepath.Join(b.TempDir(), "store.db"))
		b.Cleanup(func() { _ = s.Close() })
		return s
	})
}
