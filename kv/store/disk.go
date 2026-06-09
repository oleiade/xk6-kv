package store

import (
	"fmt"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// DiskStore is a Backend that persists key-value pairs in a BoltDB database
// on disk.
type DiskStore struct {
	path   string
	handle *bolt.DB
	bucket []byte
}

// Ensure DiskStore implements the Backend interface.
var _ Backend = (*DiskStore)(nil)

const (
	// DefaultDiskStorePath is the default path to the BoltDB database file.
	DefaultDiskStorePath = ".k6.kv"

	// DefaultKvBucket is the default bucket name for the KV store
	DefaultKvBucket = "k6"
)

// NewDiskStore opens (or creates) a BoltDB database at the given path and
// returns a ready-to-use DiskStore. An empty path falls back to
// DefaultDiskStorePath.
func NewDiskStore(path string) (*DiskStore, error) {
	if path == "" {
		path = DefaultDiskStorePath
	}

	handle, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open disk store at %q: %w", path, err)
	}

	bucket := []byte(DefaultKvBucket)
	if err := handle.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(bucket)
		return e
	}); err != nil {
		_ = handle.Close()
		return nil, fmt.Errorf("create bucket %q: %w", DefaultKvBucket, err)
	}

	return &DiskStore{path: path, handle: handle, bucket: bucket}, nil
}

// view runs fn inside a read-only transaction with the KV bucket pre-resolved.
// The bucket is guaranteed to exist by NewDiskStore and is never torn down,
// so fn can use it directly.
func (s *DiskStore) view(fn func(*bolt.Bucket) error) error {
	return s.handle.View(func(tx *bolt.Tx) error { return fn(tx.Bucket(s.bucket)) })
}

// update runs fn inside a read-write transaction with the KV bucket
// pre-resolved. See view for the bucket invariant.
func (s *DiskStore) update(fn func(*bolt.Bucket) error) error {
	return s.handle.Update(func(tx *bolt.Tx) error { return fn(tx.Bucket(s.bucket)) })
}

// Get retrieves a value from the disk store.
func (s *DiskStore) Get(key string) ([]byte, error) {
	var value []byte
	err := s.view(func(b *bolt.Bucket) error {
		// bbolt's bucket.Get returns a slice valid only for the
		// transaction's lifetime; copy so callers can hold onto it.
		if v := b.Get([]byte(key)); v != nil {
			value = append([]byte(nil), v...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("get %q: %w", key, err)
	}
	if value == nil {
		return nil, fmt.Errorf("disk store: %w: %s", ErrKeyNotFound, key)
	}
	return value, nil
}

// Set sets a value in the disk store.
func (s *DiskStore) Set(key string, value []byte) error {
	if err := s.update(func(b *bolt.Bucket) error {
		return b.Put([]byte(key), value)
	}); err != nil {
		return fmt.Errorf("set %q: %w", key, err)
	}
	return nil
}

// Delete removes a value from the disk store.
func (s *DiskStore) Delete(key string) error {
	if err := s.update(func(b *bolt.Bucket) error {
		return b.Delete([]byte(key))
	}); err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	return nil
}

// Exists checks if a given key exists.
func (s *DiskStore) Exists(key string) (bool, error) {
	var exists bool
	if err := s.view(func(b *bolt.Bucket) error {
		exists = b.Get([]byte(key)) != nil
		return nil
	}); err != nil {
		return false, fmt.Errorf("exists %q: %w", key, err)
	}
	return exists, nil
}

// Clear removes all keys from the store.
func (s *DiskStore) Clear() error {
	if err := s.update(func(b *bolt.Bucket) error {
		return b.ForEach(func(k, _ []byte) error { return b.Delete(k) })
	}); err != nil {
		return fmt.Errorf("clear disk store: %w", err)
	}
	return nil
}

// Size returns the size of the store.
func (s *DiskStore) Size() (int64, error) {
	var size int64
	if err := s.view(func(b *bolt.Bucket) error {
		size = int64(b.Stats().KeyN)
		return nil
	}); err != nil {
		return 0, fmt.Errorf("size of disk store: %w", err)
	}
	return size, nil
}

// List returns key-value pairs sorted lexicographically by key (BoltDB's
// natural cursor order), optionally filtered by prefix and capped at limit
// (when > 0).
func (s *DiskStore) List(prefix string, limit int64) ([]RawEntry, error) {
	var entries []RawEntry
	if err := s.view(func(b *bolt.Bucket) error {
		var count int64
		hasLimit := limit > 0

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			key := string(k)
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}
			if hasLimit && count >= limit {
				break
			}
			// Copy: value is only valid for the transaction's lifetime.
			value := append([]byte(nil), v...)
			entries = append(entries, RawEntry{Key: key, Value: value})
			count++
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("list disk store: %w", err)
	}
	return entries, nil
}

// Close closes the underlying BoltDB file and releases its file lock.
//
// The store is shared across all VUs in a k6 run; Close is intended to be
// called at most once per process (typically at script teardown). After
// Close, any subsequent Get/Set/etc. against this DiskStore will fail.
func (s *DiskStore) Close() error {
	return s.handle.Close()
}
