package store

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory Backend. Values are kept as raw bytes and
// guarded by an RWMutex.
type MemoryStore struct {
	mu        sync.RWMutex
	container map[string][]byte
}

// Ensure MemoryStore implements the Backend interface.
var _ Backend = (*MemoryStore)(nil)

// NewMemoryStore creates a new MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		container: map[string][]byte{},
	}
}

// Get returns the value for a given key.
func (s *MemoryStore) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.container[key]
	if !ok {
		return nil, fmt.Errorf("memory store: %w: %s", ErrKeyNotFound, key)
	}
	return value, nil
}

// Set sets the value for a given key.
func (s *MemoryStore) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.container[key] = value
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

// Size returns the number of keys in the store.
func (s *MemoryStore) Size() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.container)), nil
}

// List returns key-value pairs sorted lexicographically by key, optionally
// filtered by prefix and capped at limit (when > 0).
func (s *MemoryStore) List(prefix string, limit int64) ([]RawEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.container))
	for k := range s.container {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	if limit > 0 && int64(len(keys)) > limit {
		keys = keys[:limit]
	}

	entries := make([]RawEntry, len(keys))
	for i, k := range keys {
		entries[i] = RawEntry{Key: k, Value: s.container[k]}
	}
	return entries, nil
}

// Close is a no-op for the in-memory backend.
func (s *MemoryStore) Close() error {
	return nil
}
