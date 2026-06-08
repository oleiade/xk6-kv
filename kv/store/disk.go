package store

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// DiskStore is a Backend that persists key-value pairs in a BoltDB database
// on disk.
type DiskStore struct {
	path     string
	handle   *bolt.DB
	bucket   []byte
	opened   atomic.Bool
	refCount atomic.Int64
	lock     sync.Mutex
}

// Ensure DiskStore implements the Backend interface.
var _ Backend = (*DiskStore)(nil)

const (
	// DefaultDiskStorePath is the default path to the BoltDB database file.
	DefaultDiskStorePath = ".k6.kv"

	// DefaultKvBucket is the default bucket name for the KV store
	DefaultKvBucket = "k6"
)

// NewDiskStore creates a new DiskStore that will open its BoltDB database at
// the given path on the first operation. An empty path falls back to
// DefaultDiskStorePath.
func NewDiskStore(path string) *DiskStore {
	if path == "" {
		path = DefaultDiskStorePath
	}
	return &DiskStore{
		path:   path,
		handle: new(bolt.DB),
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
		_, bucketErr := tx.CreateBucketIfNotExists([]byte(DefaultKvBucket))
		if bucketErr != nil {
			return fmt.Errorf("failed to create internal bucket: %w", bucketErr)
		}
		return nil
	})
	if err != nil {
		return err
	}

	s.handle = handler
	s.bucket = []byte(DefaultKvBucket)
	s.opened.Store(true)
	s.refCount.Add(1)

	return nil
}

// Get retrieves a value from the disk store.
func (s *DiskStore) Get(key string) ([]byte, error) {
	if err := s.open(); err != nil {
		return nil, fmt.Errorf("failed to open disk store: %w", err)
	}

	var value []byte
	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}
		// bbolt's bucket.Get returns a slice that is only valid for the
		// duration of the transaction; copy so callers can hold onto it.
		v := bucket.Get([]byte(key))
		if v != nil {
			value = append([]byte(nil), v...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get value from disk store: %w", err)
	}

	if value == nil {
		return nil, fmt.Errorf("disk store: %w: %s", ErrKeyNotFound, key)
	}
	return value, nil
}

// Set sets a value in the disk store.
func (s *DiskStore) Set(key string, value []byte) error {
	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		return bucket.Put([]byte(key), value)
	})
	if err != nil {
		return fmt.Errorf("unable to insert value into disk store: %w", err)
	}
	return nil
}

// Delete removes a value from the disk store.
func (s *DiskStore) Delete(key string) error {
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

// List returns key-value pairs sorted lexicographically by key (BoltDB's
// natural cursor order), optionally filtered by prefix and capped at limit
// (when > 0).
func (s *DiskStore) List(prefix string, limit int64) ([]RawEntry, error) {
	if err := s.open(); err != nil {
		return nil, fmt.Errorf("failed to open disk store: %w", err)
	}

	var entries []RawEntry
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
			// Copy the value: it's only valid for the transaction's lifetime.
			value := append([]byte(nil), v...)
			entries = append(entries, RawEntry{Key: key, Value: value})
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

	if s.refCount.Add(-1) > 0 {
		return nil
	}

	if err := s.handle.Close(); err != nil {
		return err
	}
	s.opened.Store(false)
	return nil
}
