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
	path          string
	handle        *bolt.DB
	bucket        []byte
	versionBucket []byte
	opened        atomic.Bool
	refCount      atomic.Int64
	lock          sync.Mutex
}

const (
	// DefaultDiskStorePath is the default path to the BoltDB database file.
	DefaultDiskStorePath = ".k6.kv"

	// DefaultKvBucket is the default bucket name for the KV store
	DefaultKvBucket = "k6"

	// DefaultKvVersionBucket is the default bucket name for per-key versionstamps.
	DefaultKvVersionBucket = "k6_versions"
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
		bucket, bucketErr := tx.CreateBucketIfNotExists([]byte(DefaultKvBucket))
		if bucketErr != nil {
			return fmt.Errorf("failed to create internal bucket: %w", bucketErr)
		}

		versions, bucketErr := tx.CreateBucketIfNotExists([]byte(DefaultKvVersionBucket))
		if bucketErr != nil {
			return fmt.Errorf("failed to create internal versions bucket: %w", bucketErr)
		}

		return backfillVersionstamps(tx, bucket, versions)
	})
	if err != nil {
		return err
	}

	s.handle = handler
	s.bucket = []byte(DefaultKvBucket)
	s.versionBucket = []byte(DefaultKvVersionBucket)
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

		value = cloneBytes(bucket.Get([]byte(key)))
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

// GetEntry returns the value and versionstamp for a key.
func (s *DiskStore) GetEntry(key string) (Entry, error) {
	if err := s.open(); err != nil {
		return Entry{}, fmt.Errorf("failed to open disk store: %w", err)
	}

	entry := Entry{Key: key}
	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		versions := tx.Bucket(s.versionBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}
		if versions == nil {
			return fmt.Errorf("bucket %s not found", s.versionBucket)
		}

		value := bucket.Get([]byte(key))
		if value == nil {
			return nil
		}

		entry.Value = cloneBytes(value)
		entry.Versionstamp = string(versions.Get([]byte(key)))
		return nil
	})
	if err != nil {
		return Entry{}, fmt.Errorf("unable to get entry from disk store: %w", err)
	}

	return entry, nil
}

// GetMany returns entries for the given keys in input order.
func (s *DiskStore) GetMany(keys []string) ([]Entry, error) {
	if err := s.open(); err != nil {
		return nil, fmt.Errorf("failed to open disk store: %w", err)
	}

	entries := make([]Entry, len(keys))
	err := s.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		versions := tx.Bucket(s.versionBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}
		if versions == nil {
			return fmt.Errorf("bucket %s not found", s.versionBucket)
		}

		for i, key := range keys {
			entries[i] = Entry{Key: key}
			value := bucket.Get([]byte(key))
			if value == nil {
				continue
			}
			entries[i].Value = cloneBytes(value)
			entries[i].Versionstamp = string(versions.Get([]byte(key)))
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get entries from disk store: %w", err)
	}

	return entries, nil
}

// Set sets a value in the disk store.
func (s *DiskStore) Set(key string, value any) error {
	// Ensure the store is open
	if err := s.open(); err != nil {
		return fmt.Errorf("failed to open disk store: %w", err)
	}

	valueBytes, err := bytesFromValue(value, "disk store")
	if err != nil {
		return err
	}

	// Update the value in the database within a BoltDB transaction
	err = s.handle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		versions := tx.Bucket(s.versionBucket)
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		if versions == nil {
			return fmt.Errorf("versions bucket not found")
		}

		if err := bucket.Put([]byte(key), valueBytes); err != nil {
			return err
		}

		return versions.Put([]byte(key), []byte(formatDiskVersionstamp(tx)))
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
		versions := tx.Bucket(s.versionBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}
		if versions == nil {
			return fmt.Errorf("bucket %s not found", s.versionBucket)
		}

		if err := bucket.Delete([]byte(key)); err != nil {
			return err
		}

		return versions.Delete([]byte(key))
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
		versions := tx.Bucket(s.versionBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}
		if versions == nil {
			return fmt.Errorf("bucket %s not found", s.versionBucket)
		}

		if err := bucket.ForEach(func(k, _ []byte) error {
			return bucket.Delete(k)
		}); err != nil {
			return err
		}

		return versions.ForEach(func(k, _ []byte) error {
			return versions.Delete(k)
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
		versions := tx.Bucket(s.versionBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}
		if versions == nil {
			return fmt.Errorf("bucket %s not found", s.versionBucket)
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
		versions := tx.Bucket(s.versionBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", s.bucket)
		}
		if versions == nil {
			return fmt.Errorf("bucket %s not found", s.versionBucket)
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
				Key:          key,
				Value:        cloneBytes(v),
				Versionstamp: string(versions.Get(k)),
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

// AtomicCommit commits checks and mutations as a single atomic operation.
func (s *DiskStore) AtomicCommit(checks []Check, mutations []Mutation) (CommitResult, error) {
	if err := s.open(); err != nil {
		return CommitResult{}, fmt.Errorf("failed to open disk store: %w", err)
	}

	var result CommitResult
	err := s.handle.Update(func(tx *bolt.Tx) error {
		bucket, versions, bucketErr := s.atomicBuckets(tx)
		if bucketErr != nil {
			return bucketErr
		}

		if !checksMatch(versions, checks) {
			result = CommitResult{Ok: false}
			return nil
		}

		versionstamp := formatDiskVersionstamp(tx)
		if mutationErr := applyDiskMutations(bucket, versions, versionstamp, mutations); mutationErr != nil {
			return mutationErr
		}
		result = CommitResult{Ok: true, Versionstamp: versionstamp}

		return nil
	})
	if err != nil {
		return CommitResult{}, fmt.Errorf("unable to commit atomic operation to disk store: %w", err)
	}

	return result, nil
}

func (s *DiskStore) atomicBuckets(tx *bolt.Tx) (*bolt.Bucket, *bolt.Bucket, error) {
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		return nil, nil, fmt.Errorf("bucket %s not found", s.bucket)
	}

	versions := tx.Bucket(s.versionBucket)
	if versions == nil {
		return nil, nil, fmt.Errorf("bucket %s not found", s.versionBucket)
	}

	return bucket, versions, nil
}

func checksMatch(versions *bolt.Bucket, checks []Check) bool {
	for _, check := range checks {
		current := string(versions.Get([]byte(check.Key)))
		if current != check.Versionstamp {
			return false
		}
	}

	return true
}

func applyDiskMutations(bucket, versions *bolt.Bucket, versionstamp string, mutations []Mutation) error {
	for _, mutation := range mutations {
		if err := applyDiskMutation(bucket, versions, versionstamp, mutation); err != nil {
			return err
		}
	}

	return nil
}

func applyDiskMutation(bucket, versions *bolt.Bucket, versionstamp string, mutation Mutation) error {
	switch mutation.Type {
	case MutationSet:
		return setDiskMutation(bucket, versions, versionstamp, mutation)
	case MutationDelete:
		return deleteDiskMutation(bucket, versions, mutation.Key)
	default:
		return fmt.Errorf("unsupported mutation type: %s", mutation.Type)
	}
}

func setDiskMutation(bucket, versions *bolt.Bucket, versionstamp string, mutation Mutation) error {
	valueBytes, err := bytesFromValue(mutation.Value, "disk store")
	if err != nil {
		return err
	}
	if err := bucket.Put([]byte(mutation.Key), valueBytes); err != nil {
		return err
	}

	return versions.Put([]byte(mutation.Key), []byte(versionstamp))
}

func deleteDiskMutation(bucket, versions *bolt.Bucket, key string) error {
	if err := bucket.Delete([]byte(key)); err != nil {
		return err
	}

	return versions.Delete([]byte(key))
}

func formatDiskVersionstamp(tx *bolt.Tx) string {
	return fmt.Sprintf("%020d", tx.ID())
}

// backfillVersionstamps assigns a versionstamp to every key written by a
// pre-versionstamp release. Such keys live in the data bucket with no entry in
// the versions bucket; left untouched they would read back as absent and an
// atomic absent-check would match a key that actually holds a value. Keys that
// already carry a versionstamp are left untouched, so this is a no-op once a
// store has been migrated.
func backfillVersionstamps(tx *bolt.Tx, bucket, versions *bolt.Bucket) error {
	var missing [][]byte
	err := bucket.ForEach(func(k, _ []byte) error {
		if versions.Get(k) == nil {
			// ForEach reuses the key slice across iterations; copy it.
			missing = append(missing, append([]byte(nil), k...))
		}

		return nil
	})
	if err != nil {
		return err
	}

	versionstamp := []byte(formatDiskVersionstamp(tx))
	for _, key := range missing {
		if err := versions.Put(key, versionstamp); err != nil {
			return err
		}
	}

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
