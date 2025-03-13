// Package store provides a key-value store interface and implementations.
package store

// Store interface defines the operations for a key-value store.
type Store interface {
	// Get returns the value of a key in the store.
	Get(key string) (any, error)

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
}

// Entry represents a key-value pair in the store.
type Entry struct {
	Key   string
	Value any
}
