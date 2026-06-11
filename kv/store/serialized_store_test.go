package store

import (
	"strings"
	"testing"
)

// fixedEntryStore returns a single canned entry from GetEntry regardless of the
// key. The embedded Store interface satisfies the rest of the contract; only
// GetEntry is exercised by these tests, so the nil embed is never called.
type fixedEntryStore struct {
	Store
	entry Entry
}

func (s fixedEntryStore) GetEntry(string) (Entry, error) {
	return s.entry, nil
}

// TestSerializedStore_PresentEntryWithoutVersionstamp proves that presence is
// read from Entry.Found, not from the versionstamp. A present value carrying no
// versionstamp must still be returned rather than reported as missing.
func TestSerializedStore_PresentEntryWithoutVersionstamp(t *testing.T) {
	t.Parallel()

	backend := fixedEntryStore{entry: Entry{Key: "key", Value: []byte(`"value"`), Found: true}}
	store := NewSerializedStore(backend, NewJSONSerializer())

	got, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get() on a present unversioned entry returned an error: %v", err)
	}
	if got != "value" {
		t.Fatalf("Get() returned unexpected value, got %#v, want %q", got, "value")
	}
}

// TestSerializedStore_AbsentEntryWithVersionstamp proves the converse: an absent
// entry must be reported as missing even if it carries a stale versionstamp, and
// its value must never be deserialized.
func TestSerializedStore_AbsentEntryWithVersionstamp(t *testing.T) {
	t.Parallel()

	backend := fixedEntryStore{entry: Entry{
		Key:          "key",
		Value:        []byte("not-valid-json"),
		Versionstamp: "00000000000000000001",
		Found:        false,
	}}
	store := NewSerializedStore(backend, NewJSONSerializer())

	_, err := store.Get("key")
	if err == nil {
		t.Fatal("Get() on an absent entry should return an error")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Get() returned unexpected error, got %v, want a not-found error", err)
	}
}
