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
	entry, err := s.store.GetEntry(key)
	if err != nil {
		return nil, err
	}
	if entry.Versionstamp == "" {
		return nil, fmt.Errorf("key %s not found", key)
	}

	return s.deserializeValue(entry.Value)
}

// GetEntry retrieves an entry from the store and deserializes its value.
func (s *SerializedStore) GetEntry(key string) (Entry, error) {
	entry, err := s.store.GetEntry(key)
	if err != nil {
		return Entry{}, err
	}
	if entry.Versionstamp == "" {
		return entry, nil
	}

	entry.Value, err = s.deserializeValue(entry.Value)
	if err != nil {
		return Entry{}, fmt.Errorf("failed to deserialize value for key %s: %w", entry.Key, err)
	}

	return entry, nil
}

// GetMany retrieves entries from the store and deserializes their values.
func (s *SerializedStore) GetMany(keys []string) ([]Entry, error) {
	entries, err := s.store.GetMany(keys)
	if err != nil {
		return nil, err
	}

	return s.deserializeEntries(entries)
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
	return s.deserializeEntries(rawEntries)
}

// Close closes the underlying store.
func (s *SerializedStore) Close() error {
	return s.store.Close()
}

// AtomicCommit serializes set mutations and commits them to the underlying store.
func (s *SerializedStore) AtomicCommit(checks []Check, mutations []Mutation) (CommitResult, error) {
	serializedMutations := make([]Mutation, len(mutations))
	for i, mutation := range mutations {
		serializedMutations[i] = mutation
		if mutation.Type != MutationSet {
			continue
		}

		serializedValue, err := s.serializer.Serialize(mutation.Value)
		if err != nil {
			return CommitResult{}, fmt.Errorf("failed to serialize value for key %s: %w", mutation.Key, err)
		}
		serializedMutations[i].Value = serializedValue
	}

	return s.store.AtomicCommit(checks, serializedMutations)
}

func (s *SerializedStore) deserializeEntries(rawEntries []Entry) ([]Entry, error) {
	entries := make([]Entry, len(rawEntries))
	for i, entry := range rawEntries {
		if entry.Versionstamp == "" {
			entries[i] = entry
			continue
		}

		deserializedValue, err := s.deserializeValue(entry.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize value for key %s: %w", entry.Key, err)
		}
		entry.Value = deserializedValue
		entries[i] = entry
	}

	return entries, nil
}

func (s *SerializedStore) deserializeValue(rawValue any) (any, error) {
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
