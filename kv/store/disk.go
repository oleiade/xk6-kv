package store

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// DiskStore is a key-value store that uses a BoltDB database on disk.
type DiskStore struct {
	path     string
	handle   *bolt.DB
	bucket   []byte
	opened   atomic.Bool
	refCount atomic.Int64
	lock     sync.Mutex
}

const (
	// DefaultDiskStorePath is the default path to the BoltDB database file.
	DefaultDiskStorePath = ".k6.kv"

	// DefaultKvBucket is the default bucket name for the KV store
	DefaultKvBucket = "k6"
)

// NewDiskStore creates a new DiskStore instance.
func NewDiskStore() *DiskStore {
	return &DiskStore{
		path:     DefaultDiskStorePath,
		handle:   new(bolt.DB),
		opened:   atomic.Bool{},
		refCount: atomic.Int64{},
		lock:     sync.Mutex{},
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

	return nil
}

// Get retrieves a value from the disk store.
func (s *DiskStore) Get(key string) (any, error) {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return nil, fmt.Errorf("failed to open disk store: %w", err)
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

// Set sets a value in the disk store.
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

	return nil
}

// Delete removes a value from the disk store.
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

	return nil
}

// Exists checks if a given key exists.
func (s *DiskStore) Exists(key string) (bool, error) {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return false, fmt.Errorf("failed to open disk store: %w", err)
	}

	exists := false
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

// Clear removes all keys from the store.
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
