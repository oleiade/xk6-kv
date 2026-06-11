// Package store provides a key-value store interface and implementations.
package store

import "fmt"

// Store interface defines the operations for a key-value store.
//
//nolint:interfacebloat // Store is the backend contract shared by all KV implementations.
type Store interface {
	// Get returns the value of a key in the store.
	Get(key string) (any, error)

	// GetEntry returns the key's value and versionstamp.
	//
	// If the key does not exist, the returned entry has Found set to false,
	// a nil value, and an empty versionstamp.
	GetEntry(key string) (Entry, error)

	// GetMany returns entries for keys in the same order as the input keys.
	GetMany(keys []string) ([]Entry, error)

	// Set sets the value of a key in the store.
	Set(key string, value any) error

	// Delete deletes a key from the store.
	Delete(key string) error

	// Exists checks if a given key exists.
	Exists(key string) (bool, error)

	// Clear clears the store.
	Clear() error

	// Size returns the number of keys in the store.
	Size() (int64, error)

	// List returns all key-value pairs in the store, optionally filtered by prefix and limited to a maximum count.
	List(prefix string, limit int64) ([]Entry, error)

	// Close closes the store.
	Close() error

	// AtomicCommit commits checks and mutations as a single atomic operation.
	AtomicCommit(checks []Check, mutations []Mutation) (CommitResult, error)
}

// Entry represents a key-value pair in the store.
type Entry struct {
	Key          string
	Value        any
	Versionstamp string

	// Found reports whether the key exists in the store. It distinguishes a
	// present key from an absent one without relying on the versionstamp.
	Found bool
}

// Check represents a versionstamp precondition for an atomic commit.
type Check struct {
	Key          string
	Versionstamp string
}

// MutationType identifies the kind of mutation in an atomic commit.
type MutationType string

const (
	// MutationSet sets the key to the provided value.
	MutationSet MutationType = "set"

	// MutationDelete deletes the key.
	MutationDelete MutationType = "delete"
)

// Mutation represents a state change in an atomic commit.
type Mutation struct {
	Type  MutationType
	Key   string
	Value any
}

// CommitResult is returned from an atomic commit.
type CommitResult struct {
	Ok           bool
	Versionstamp string
}

func formatVersionstamp(version uint64) string {
	return fmt.Sprintf("%020d", version)
}
