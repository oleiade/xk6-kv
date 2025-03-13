package store

import "fmt"

// SerializedStore wraps a Store and adds serialization capabilities.
type SerializedStore struct {
	// The underlying store implementation
	store Store

	// The serializer to use
	serializer Serializer
}

// NewSerializedStore creates a new SerializedStore with the given store and serializer.
func NewSerializedStore(store Store, serializer Serializer) *SerializedStore {
	return &SerializedStore{
		store:      store,
		serializer: serializer,
	}
}

// Get retrieves a value from the store and deserializes it.
func (s *SerializedStore) Get(key string) (any, error) {
	// Get the raw value from the store
	rawValue, err := s.store.Get(key)
	if err != nil {
		return nil, err
	}

	// Handle string values from stores that don't use byte slices
	if strValue, ok := rawValue.(string); ok {
		return s.serializer.Deserialize([]byte(strValue))
	}

	// Handle byte slice values
	if byteValue, ok := rawValue.([]byte); ok {
		return s.serializer.Deserialize(byteValue)
	}

	// If the value is already deserialized (e.g., from memory store)
	return rawValue, nil
}

// Set serializes a value and stores it.
func (s *SerializedStore) Set(key string, value any) error {
	// Serialize the value
	serializedValue, err := s.serializer.Serialize(value)
	if err != nil {
		return fmt.Errorf("failed to serialize value: %w", err)
	}

	// Store the serialized value
	return s.store.Set(key, serializedValue)
}

// Delete removes a key from the store.
func (s *SerializedStore) Delete(key string) error {
	return s.store.Delete(key)
}

// Exists checks if a key exists in the store.
func (s *SerializedStore) Exists(key string) (bool, error) {
	return s.store.Exists(key)
}

// Clear removes all keys from the store.
func (s *SerializedStore) Clear() error {
	return s.store.Clear()
}

// Size returns the number of keys in the store.
func (s *SerializedStore) Size() (int64, error) {
	return s.store.Size()
}

// List returns all key-value pairs in the store, optionally filtered by prefix and limited to a maximum count.
func (s *SerializedStore) List(prefix string, limit int64) ([]Entry, error) {
	// Get the raw entries from the underlying store
	rawEntries, err := s.store.List(prefix, limit)
	if err != nil {
		return nil, err
	}

	// Deserialize each entry's value
	entries := make([]Entry, len(rawEntries))
	for i, entry := range rawEntries {
		// Handle string values from stores that don't use byte slices
		if strValue, ok := entry.Value.(string); ok {
			deserializedValue, err := s.serializer.Deserialize([]byte(strValue))
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize value for key %s: %w", entry.Key, err)
			}
			entries[i] = Entry{Key: entry.Key, Value: deserializedValue}
			continue
		}

		// Handle byte slice values
		if byteValue, ok := entry.Value.([]byte); ok {
			deserializedValue, err := s.serializer.Deserialize(byteValue)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize value for key %s: %w", entry.Key, err)
			}
			entries[i] = Entry{Key: entry.Key, Value: deserializedValue}
			continue
		}

		// If the value is already deserialized (e.g., from memory store)
		entries[i] = entry
	}

	return entries, nil
}

// Close closes the underlying store.
func (s *SerializedStore) Close() error {
	return s.store.Close()
}

// GetSerializer returns the serializer used by this store.
func (s *SerializedStore) GetSerializer() Serializer {
	return s.serializer
}

// SetSerializer changes the serializer used by this store.
func (s *SerializedStore) SetSerializer(serializer Serializer) {
	s.serializer = serializer
}
