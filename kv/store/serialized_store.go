package store

import "fmt"

// SerializedStore is the canonical Store implementation: it wraps a Backend
// and a Serializer, exposing JS-friendly `any`-typed values on top of the
// backend's raw bytes.
type SerializedStore struct {
	backend    Backend
	serializer Serializer
}

// Ensure SerializedStore implements the Store interface.
var _ Store = (*SerializedStore)(nil)

// NewSerializedStore creates a new SerializedStore with the given backend
// and serializer.
func NewSerializedStore(backend Backend, serializer Serializer) *SerializedStore {
	return &SerializedStore{
		backend:    backend,
		serializer: serializer,
	}
}

// Get retrieves and deserializes the value for a key.
func (s *SerializedStore) Get(key string) (any, error) {
	raw, err := s.backend.Get(key)
	if err != nil {
		return nil, err
	}
	return s.serializer.Deserialize(raw)
}

// Set serializes a value and stores it.
func (s *SerializedStore) Set(key string, value any) error {
	raw, err := s.serializer.Serialize(value)
	if err != nil {
		return fmt.Errorf("failed to serialize value: %w", err)
	}
	return s.backend.Set(key, raw)
}

// Delete removes a key from the store.
func (s *SerializedStore) Delete(key string) error {
	return s.backend.Delete(key)
}

// Exists checks if a key exists in the store.
func (s *SerializedStore) Exists(key string) (bool, error) {
	return s.backend.Exists(key)
}

// Clear removes all keys from the store.
func (s *SerializedStore) Clear() error {
	return s.backend.Clear()
}

// Size returns the number of keys in the store.
func (s *SerializedStore) Size() (int64, error) {
	return s.backend.Size()
}

// List returns deserialized key-value pairs, optionally filtered by prefix
// and capped at limit (when > 0).
func (s *SerializedStore) List(prefix string, limit int64) ([]Entry, error) {
	raw, err := s.backend.List(prefix, limit)
	if err != nil {
		return nil, err
	}

	entries := make([]Entry, len(raw))
	for i, e := range raw {
		value, derr := s.serializer.Deserialize(e.Value)
		if derr != nil {
			return nil, fmt.Errorf("failed to deserialize value for key %s: %w", e.Key, derr)
		}
		entries[i] = Entry{Key: e.Key, Value: value}
	}
	return entries, nil
}

// Close closes the underlying backend.
func (s *SerializedStore) Close() error {
	return s.backend.Close()
}
