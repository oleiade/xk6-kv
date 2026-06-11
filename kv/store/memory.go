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
	versions  map[string]string
	version   uint64
}

// NewMemoryStore creates a new MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mu:        sync.RWMutex{},
		container: map[string][]byte{},
		versions:  map[string]string{},
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
	return cloneBytes(value), nil
}

// GetEntry returns the value and versionstamp for a given key.
func (s *MemoryStore) GetEntry(key string) (Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.container[key]
	if !ok {
		return Entry{Key: key}, nil
	}

	return Entry{
		Key:          key,
		Value:        cloneBytes(value),
		Versionstamp: s.versions[key],
		Found:        true,
	}, nil
}

// GetMany returns entries for the given keys in input order.
func (s *MemoryStore) GetMany(keys []string) ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]Entry, len(keys))
	for i, key := range keys {
		entries[i] = Entry{Key: key}
		if value, ok := s.container[key]; ok {
			entries[i].Value = cloneBytes(value)
			entries[i].Versionstamp = s.versions[key]
			entries[i].Found = true
		}
	}

	return entries, nil
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

	s.container[key] = cloneBytes(valueBytes)
	s.versions[key] = s.nextVersionstamp()
	return nil
}

// Delete deletes the value for a given key.
func (s *MemoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.container, key)
	delete(s.versions, key)
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
	s.versions = map[string]string{}
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

	var entries []Entry
	var count int64

	// Apply limit if set
	hasLimit := limit > 0
	for _, k := range keys {
		if hasLimit && count >= limit {
			break
		}

		entries = append(entries, Entry{
			Key:          k,
			Value:        cloneBytes(s.container[k]),
			Versionstamp: s.versions[k],
			Found:        true,
		})
		count++
	}

	return entries, nil
}

// AtomicCommit commits checks and mutations as a single atomic operation.
func (s *MemoryStore) AtomicCommit(checks []Check, mutations []Mutation) (CommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, check := range checks {
		if s.versions[check.Key] != check.Versionstamp {
			return CommitResult{Ok: false}, nil
		}
	}

	versionstamp := s.nextVersionstamp()
	for _, mutation := range mutations {
		switch mutation.Type {
		case MutationSet:
			valueBytes, err := bytesFromValue(mutation.Value, "memory store")
			if err != nil {
				return CommitResult{}, err
			}
			s.container[mutation.Key] = cloneBytes(valueBytes)
			s.versions[mutation.Key] = versionstamp
		case MutationDelete:
			delete(s.container, mutation.Key)
			delete(s.versions, mutation.Key)
		default:
			return CommitResult{}, fmt.Errorf("unsupported mutation type: %s", mutation.Type)
		}
	}

	return CommitResult{Ok: true, Versionstamp: versionstamp}, nil
}

// Close closes the store.
//
// This is a no-op for the MemoryStore.
func (s *MemoryStore) Close() error {
	return nil
}

func (s *MemoryStore) nextVersionstamp() string {
	s.version++
	return formatVersionstamp(s.version)
}

func bytesFromValue(value any, storeName string) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("unsupported value type for %s: %T", storeName, value)
	}
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
