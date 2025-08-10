package store

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory key-value store.
type MemoryStore struct {
	mu        sync.RWMutex
	container map[string][]byte

	// Optional key tracking
	trackKeys bool
	keysList  []string       // slice of keys for random access
	keysMap   map[string]int // map from key to index in keysList
}

// NewMemoryStore creates a new MemoryStore.
func NewMemoryStore(trackKeys bool) *MemoryStore {
	return &MemoryStore{
		mu:        sync.RWMutex{},
		container: map[string][]byte{},
		trackKeys: trackKeys,
		keysList:  []string{},
		keysMap:   make(map[string]int),
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

	_, existed := s.container[key]
	s.container[key] = valueBytes

	if s.trackKeys && !existed {
		s.keysMap[key] = len(s.keysList)
		s.keysList = append(s.keysList, key)
	}

	return nil
}

// Delete deletes the value for a given key.
func (s *MemoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.container, key)

	if s.trackKeys {
		if idx, exists := s.keysMap[key]; exists {
			lastIndex := len(s.keysList) - 1
			lastKey := s.keysList[lastIndex]
			if idx != lastIndex {
				s.keysList[idx] = lastKey
				s.keysMap[lastKey] = idx
			}
			s.keysList = s.keysList[:lastIndex]
			delete(s.keysMap, key)
		}
	}

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

	if s.trackKeys {
		s.keysList = []string{}
		s.keysMap = make(map[string]int)
	}

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

// RandomKey returns a random key from the store.
// Returns "" and nil error when the store is empty.
// May be O(1) when in-memory key tracking is enabled.
// Otherwise the implementation may fall back to a slower scan.
func (s *MemoryStore) RandomKey() (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.trackKeys {
		if len(s.keysList) == 0 {
			return "", nil
		}

		idx := rand.IntN(len(s.keysList)) //nolint:gosec

		return s.keysList[idx], nil
	}

	if len(s.container) == 0 {
		return "", nil
	}

	// fallback: build list on the fly
	keys := make([]string, 0, len(s.container))
	for k := range s.container {
		keys = append(keys, k)
	}

	idx := rand.IntN(len(keys)) //nolint:gosec

	return keys[idx], nil
}

// RebuildKeyList rebuilds the in-memory key list from the container.
func (s *MemoryStore) RebuildKeyList() error {
	if !s.trackKeys {
		return nil
	}

	s.mu.Lock()

	defer s.mu.Unlock()

	s.keysList = make([]string, 0, len(s.container))
	s.keysMap = make(map[string]int, len(s.container))

	for k := range s.container {
		s.keysMap[k] = len(s.keysList)
		s.keysList = append(s.keysList, k)
	}

	return nil
}

// Close closes the store.
//
// This is a no-op for the MemoryStore.
func (s *MemoryStore) Close() error {
	return nil
}
