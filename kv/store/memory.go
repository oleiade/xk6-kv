package store

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory key-value store.
type MemoryStore struct {
	mu        sync.RWMutex
	container map[string][]byte
}

// NewMemoryStore creates a new MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mu:        sync.RWMutex{},
		container: map[string][]byte{},
	}
}

// Get returns the value for a given key.
func (s *MemoryStore) Get(key string) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.container[key]
	if !ok {
		return nil, fmt.Errorf("key %s not found", key)
	}

	// Return the raw bytes - serialization will be handled by the SerializedStore wrapper
	return value, nil
}

// Set sets the value for a given key.
func (s *MemoryStore) Set(key string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Convert value to bytes if it's not already
	var valueBytes []byte
	switch v := value.(type) {
	case []byte:
		valueBytes = v
	case string:
		valueBytes = []byte(v)
	default:
		return fmt.Errorf("unsupported value type for memory store: %T", value)
	}

	s.container[key] = valueBytes
	return nil
}

// Delete deletes the value for a given key.
func (s *MemoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.container, key)
	return nil
}

// Exists checks if a given key exists.
func (s *MemoryStore) Exists(key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.container[key]
	return ok, nil
}

// Clear clears the store.
func (s *MemoryStore) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.container = map[string][]byte{}
	return nil
}

// Size returns the size of the store.
func (s *MemoryStore) Size() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.container)), nil
}

// List returns all key-value pairs in the store, optionally filtered by prefix and limited to a maximum count.
func (s *MemoryStore) List(prefix string, limit int64) ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Sort keys for consistent ordering
	keys := make([]string, 0, len(s.container))
	for k := range s.container {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var entries []Entry //nolint:prealloc
	var count int64

	// Apply limit if set
	hasLimit := limit > 0
	for _, k := range keys {
		if hasLimit && count >= limit {
			break
		}

		entries = append(entries, Entry{
			Key:   k,
			Value: s.container[k],
		})
		count++
	}

	return entries, nil
}

// Close closes the store.
//
// This is a no-op for the MemoryStore.
func (s *MemoryStore) Close() error {
	return nil
}
