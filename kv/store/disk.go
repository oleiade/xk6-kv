package store

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// DiskStore is a key-value store that uses a BoltDB database on disk.
// It supports optional in-memory key tracking for O(1) RandomKey lookups.
type DiskStore struct {
	path     string
	handle   *bolt.DB
	bucket   []byte
	opened   atomic.Bool
	refCount atomic.Int64
	lock     sync.Mutex

	// Key tracking fields (enabled via trackKeys):
	trackKeys bool           // whether in-memory key tracking is enabled
	keysList  []string       // slice of all keys for O(1) random access by index
	keysMap   map[string]int // maps key to its index in keysList for O(1) deletion
	keysLock  sync.RWMutex   // mutex to protect concurrent access to keysList/keysMap
}

const (
	// DefaultDiskStorePath is the default path to the BoltDB database file.
	DefaultDiskStorePath = ".k6.kv"

	// DefaultKvBucket is the default bucket name for the KV store
	DefaultKvBucket = "k6"
)

// NewDiskStore creates a new DiskStore instance.
// If trackKeys is true, the store will maintain an in-memory index of all keys.
func NewDiskStore(trackKeys bool) *DiskStore {
	return &DiskStore{
		path:      DefaultDiskStorePath,
		handle:    new(bolt.DB),
		opened:    atomic.Bool{},
		refCount:  atomic.Int64{},
		lock:      sync.Mutex{},
		trackKeys: trackKeys,
		keysMap:   make(map[string]int),
		keysList:  []string{},
		keysLock:  sync.RWMutex{},
	}
}

// open opens the database if it is not already open.
//
// It is safe to call this method multiple times.
// The database will only be opened once.
func (s *DiskStore) open() error {
	if s.opened.Load() {
		s.refCount.Add(1)
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if s.opened.Load() {
		return nil
	}

	handler, err := bolt.Open(s.path, 0o600, nil)
	if err != nil {
		return err
	}

	err = handler.Update(func(tx *bolt.Tx) error {
		_, bucketErr := tx.CreateBucketIfNotExists([]byte(DefaultDiskStorePath))
		if bucketErr != nil {
			return fmt.Errorf("failed to create internal bucket: %w", bucketErr)
		}

		return nil
	})
	if err != nil {
		return err
	}

	s.handle = handler
	s.bucket = []byte(DefaultDiskStorePath)
	s.opened.Store(true)
	s.refCount.Add(1)

	// If in-memory key tracking is enabled, load all existing keys from DB into memory.
	if s.trackKeys {
		if err := s.rebuildKeyListUnlocked(); err != nil {
			// If we fail to build the key list, close DB and return error.
			_ = s.handle.Close()
			s.opened.Store(false)

			return fmt.Errorf("failed to initialize key list: %w", err)
		}
	}

	return nil
}

// Get retrieves a value from the disk store.
func (s *DiskStore) Get(key string) (any, error) {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return nil, fmt.Errorf("failed to open disk store: %w", err)
	}

	if s.trackKeys {
		s.keysLock.RLock()
		_, ok := s.keysMap[key]
		s.keysLock.RUnlock()

		if !ok {
			return nil, fmt.Errorf("key %s not found", key)
		}
	}

	var value []byte

	// Get the value from the database within a BoltDB transaction
	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		value = bucket.Get([]byte(key))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get value from disk store: %w", err)
	}

	if value == nil {
		return nil, fmt.Errorf("key %s not found", key)
	}

	// Return the raw bytes - serialization will be handled by the SerializedStore wrapper
	return value, nil
}

// Set inserts or updates the value for a given key.
// If in-memory key tracking is enabled, it adds the key to the in-memory index if it is a new key.
func (s *DiskStore) Set(key string, value any) error {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	// Convert value to bytes if it's not already
	var valueBytes []byte
	switch v := value.(type) {
	case []byte:
		valueBytes = v
	case string:
		valueBytes = []byte(v)
	default:
		return fmt.Errorf("unsupported value type for disk store: %T", value)
	}

	// Update the value in the database within a BoltDB transaction
	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		return bucket.Put([]byte(key), valueBytes)
	})
	if err != nil {
		return fmt.Errorf("unable to insert value into disk store: %w", err)
	}

	// Update the in-memory key list if tracking is enabled
	if s.trackKeys {
		s.keysLock.Lock()
		if _, exists := s.keysMap[key]; !exists {
			// New key: append to keysList and record its index in keysMap
			s.keysMap[key] = len(s.keysList)
			s.keysList = append(s.keysList, key)
		}

		s.keysLock.Unlock()
	}

	return nil
}

// Delete removes a key and its value from the store.
// If key tracking is enabled, the key is also removed from the in-memory index in O(1) time.
func (s *DiskStore) Delete(key string) error {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		return bucket.Delete([]byte(key))
	})
	if err != nil {
		return fmt.Errorf("unable to delete value from disk store: %w", err)
	}

	// If tracking is enabled, remove the key from the in-memory structures
	if s.trackKeys {
		s.keysLock.Lock()
		if idx, exists := s.keysMap[key]; exists {
			lastIndex := len(s.keysList) - 1
			lastKey := s.keysList[lastIndex]
			// Swap the element to delete with the last element, to enable O(1) removal
			if idx != lastIndex {
				s.keysList[idx] = lastKey
				s.keysMap[lastKey] = idx
			}

			// Remove the last element (which is now the target key)
			s.keysList = s.keysList[:lastIndex]

			delete(s.keysMap, key)
		}

		s.keysLock.Unlock()
	}

	return nil
}

// Exists checks if a given key exists.
func (s *DiskStore) Exists(key string) (bool, error) {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return false, fmt.Errorf("failed to open disk store: %w", err)
	}

	if s.trackKeys {
		s.keysLock.RLock()
		_, ok := s.keysMap[key]
		s.keysLock.RUnlock()

		return ok, nil
	}

	var exists bool
	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		exists = bucket.Get([]byte(key)) != nil
		return nil
	})
	if err != nil {
		return exists, fmt.Errorf("unable to check if key exists in disk store: %w", err)
	}

	return exists, nil
}

// Clear wipes all keys and values from the store.
// It also clears the in-memory key list if tracking is enabled.
func (s *DiskStore) Clear() error {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		return bucket.ForEach(func(k, _ []byte) error {
			return bucket.Delete(k)
		})
	})
	if err != nil {
		return fmt.Errorf("unable to clear disk store: %w", err)
	}

	// Reset the in-memory key tracking structures
	if s.trackKeys {
		s.keysLock.Lock()
		s.keysList = []string{}
		s.keysMap = make(map[string]int)
		s.keysLock.Unlock()
	}

	return nil
}

// Size returns the size of the store.
func (s *DiskStore) Size() (int64, error) {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return 0, fmt.Errorf("failed to open disk store: %w", err)
	}

	var size int64

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		size = int64(bucket.Stats().KeyN)

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("unable to get size of disk store: %w", err)
	}

	return size, nil
}

// List returns all key-value pairs in the store, optionally filtered by prefix and limited to a maximum count.
func (s *DiskStore) List(prefix string, limit int64) ([]Entry, error) {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return nil, fmt.Errorf("failed to open disk store: %w", err)
	}

	var entries []Entry

	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		var count int64
		hasLimit := limit > 0

		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			key := string(k)
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}

			if hasLimit && count >= limit {
				break
			}

			entries = append(entries, Entry{
				Key:   key,
				Value: v,
			})
			count++
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list entries from disk store: %w", err)
	}

	return entries, nil
}

// RandomKey returns a random key from the store.
// Returns "" and nil error when the store is empty.
// May be O(1) when in-memory key tracking is enabled.
// Otherwise the implementation may fall back to a slower scan.
func (s *DiskStore) RandomKey() (string, error) {
	if err := s.open(); err != nil {
		return "", fmt.Errorf("failed to open disk store: %w", err)
	}

	if s.trackKeys {
		// Fast path: choose a random key from the in-memory slice
		s.keysLock.RLock()
		defer s.keysLock.RUnlock()

		if len(s.keysList) == 0 {
			return "", nil
		}

		randomIndex := rand.IntN(len(s.keysList)) //nolint:gosec

		return s.keysList[randomIndex], nil
	}

	// Slow path: use List() to get all keys
	keyCount, err := s.Size()
	if err != nil {
		return "", fmt.Errorf("failed to get store size: %w", err)
	}

	if keyCount == 0 {
		return "", nil
	}

	randomIndex := rand.Int64N(keyCount) //nolint:gosec

	entries, err := s.List("", randomIndex+1)
	if err != nil {
		return "", fmt.Errorf("failed to list keys: %w", err)
	}

	if len(entries) == 0 {
		return "", fmt.Errorf("no keys returned from list")
	}

	return entries[len(entries)-1].Key, nil
}

// RebuildKeyList re-scans all keys from BoltDB to rebuild the in-memory key index.
// This can be used to recover from any inconsistency between the in-memory list
// and the actual data on disk (for example, after a corruption or manual intervention).
func (s *DiskStore) RebuildKeyList() error {
	if !s.trackKeys {
		return nil
	}

	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	s.keysLock.Lock()
	defer s.keysLock.Unlock()

	if err := s.rebuildKeyListUnlocked(); err != nil {
		return fmt.Errorf("unable to rebuild keys from disk: %w", err)
	}

	return nil
}

// rebuildKeyListUnlocked is an internal helper that rebuilds the key list from DB.
// It assumes that any necessary locking has been handled by the caller (used during open).
func (s *DiskStore) rebuildKeyListUnlocked() error {
	// We don't lock keysLock here because this is called during initialization
	// when no other operations are in progress.
	// Alternatively, we could lock it to be safe.
	newKeys := []string{}
	newMap := make(map[string]int)
	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}

		return bucket.ForEach(func(k, _ []byte) error {
			keyStr := string(k)
			newMap[keyStr] = len(newKeys)
			newKeys = append(newKeys, keyStr)

			return nil
		})
	})
	if err != nil {
		return err
	}

	s.keysList = newKeys
	s.keysMap = newMap

	return nil
}

// Close closes the disk store.
func (s *DiskStore) Close() error {
	if !s.opened.Load() {
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// Decrement the reference count
	newCount := s.refCount.Add(-1)
	if newCount > 0 {
		// Still in use by other instances
		return nil
	}

	// Close the database
	err := s.handle.Close()
	if err != nil {
		return err
	}

	s.opened.Store(false)
	return nil
}
