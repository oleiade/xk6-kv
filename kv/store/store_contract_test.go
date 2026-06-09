package store

import (
	"errors"
	"strings"
	"testing"
)

// backendFactory produces a fresh, empty Backend for each contract subtest.
// Cleanup (closing handles, removing files) should be wired via t.Cleanup
// inside the factory if the backend needs it.
type backendFactory func(t *testing.T) Backend

// runBackendContract exercises the full Backend contract against a concrete
// implementation. Every concrete Backend must pass this suite; backend-
// specific behavior (refcounting, concurrency stress) lives alongside the
// backend's own test file.
func runBackendContract(t *testing.T, factory backendFactory) {
	t.Helper()

	t.Run("Get_missing_returns_ErrKeyNotFound", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		_, err := b.Get("absent")
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("expected errors.Is(err, ErrKeyNotFound), got %v", err)
		}
	})

	t.Run("Set_then_Get_roundtrips", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		if err := b.Set("k", []byte("v")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		got, err := b.Get("k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(got) != "v" {
			t.Fatalf("Get returned %q, want %q", got, "v")
		}
	})

	t.Run("Set_overwrites", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		_ = b.Set("k", []byte("v1"))
		_ = b.Set("k", []byte("v2"))
		got, _ := b.Get("k")
		if string(got) != "v2" {
			t.Fatalf("Get returned %q after overwrite, want %q", got, "v2")
		}
	})

	t.Run("Set_empty_value_roundtrips", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		if err := b.Set("k", []byte{}); err != nil {
			t.Fatalf("Set: %v", err)
		}
		got, err := b.Get("k")
		if errors.Is(err, ErrKeyNotFound) {
			t.Fatal("Get on key with empty value returned ErrKeyNotFound — empty must be distinguishable from missing")
		}
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("Get on key with empty value returned a nil slice; want non-nil empty slice")
		}
		if len(got) != 0 {
			t.Fatalf("Get returned %d bytes, want 0", len(got))
		}
	})

	t.Run("Delete_existing", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		_ = b.Set("k", []byte("v"))
		if err := b.Delete("k"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		exists, _ := b.Exists("k")
		if exists {
			t.Fatal("Delete did not remove the key")
		}
	})

	t.Run("Delete_missing_is_noop", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		if err := b.Delete("never-set"); err != nil {
			t.Fatalf("Delete on missing key returned error: %v", err)
		}
	})

	t.Run("Exists", func(t *testing.T) {
		t.Parallel()
		b := factory(t)

		exists, err := b.Exists("missing")
		if err != nil || exists {
			t.Fatalf("Exists(missing) = (%v, %v), want (false, nil)", exists, err)
		}

		_ = b.Set("present", []byte("v"))
		exists, err = b.Exists("present")
		if err != nil || !exists {
			t.Fatalf("Exists(present) = (%v, %v), want (true, nil)", exists, err)
		}
	})

	t.Run("Clear_empties_store", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		_ = b.Set("a", []byte("1"))
		_ = b.Set("b", []byte("2"))
		if err := b.Clear(); err != nil {
			t.Fatalf("Clear: %v", err)
		}
		size, _ := b.Size()
		if size != 0 {
			t.Fatalf("Size after Clear = %d, want 0", size)
		}
	})

	t.Run("Size_tracks_entry_count", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		size, _ := b.Size()
		if size != 0 {
			t.Fatalf("empty Size = %d, want 0", size)
		}
		_ = b.Set("a", []byte("1"))
		_ = b.Set("b", []byte("2"))
		size, _ = b.Size()
		if size != 2 {
			t.Fatalf("Size after 2 sets = %d, want 2", size)
		}
	})

	t.Run("List_lexicographic", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		_ = b.Set("c", []byte("3"))
		_ = b.Set("a", []byte("1"))
		_ = b.Set("b", []byte("2"))

		entries, err := b.List("", 0)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		want := []string{"a", "b", "c"}
		if len(entries) != len(want) {
			t.Fatalf("List returned %d entries, want %d", len(entries), len(want))
		}
		for i, e := range entries {
			if e.Key != want[i] {
				t.Fatalf("entry %d: key=%q, want %q (List is not lexicographic)", i, e.Key, want[i])
			}
		}
	})

	t.Run("List_prefix_filters", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		_ = b.Set("p-1", []byte("1"))
		_ = b.Set("p-2", []byte("2"))
		_ = b.Set("other", []byte("3"))

		entries, err := b.List("p-", 0)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("List(prefix=p-) returned %d entries, want 2", len(entries))
		}
		for _, e := range entries {
			if !strings.HasPrefix(e.Key, "p-") {
				t.Fatalf("entry %q does not have prefix %q", e.Key, "p-")
			}
		}
	})

	t.Run("List_limit_caps_results", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		_ = b.Set("a", []byte("1"))
		_ = b.Set("b", []byte("2"))
		_ = b.Set("c", []byte("3"))

		entries, err := b.List("", 2)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("List(limit=2) returned %d entries, want 2", len(entries))
		}
	})

	t.Run("List_prefix_and_limit", func(t *testing.T) {
		t.Parallel()
		b := factory(t)
		_ = b.Set("p-1", []byte("1"))
		_ = b.Set("p-2", []byte("2"))
		_ = b.Set("other", []byte("3"))

		entries, err := b.List("p-", 1)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("List(prefix=p-, limit=1) returned %d entries, want 1", len(entries))
		}
		if !strings.HasPrefix(entries[0].Key, "p-") {
			t.Fatalf("entry %q does not have prefix %q", entries[0].Key, "p-")
		}
	})
}
