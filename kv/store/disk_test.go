//go:build !windows

package store

import (
	"os"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestNewDiskStore(t *testing.T) {
	t.Parallel()

	store := NewDiskStore()
	if store == nil {
		t.Fatal("NewDiskStore() returned nil")
	}
	if store.path != DefaultDiskStorePath {
		t.Fatalf("NewDiskStore() returned a store with unexpected path, got %s, want %s", store.path, DefaultDiskStorePath)
	}
	if store.handle == nil {
		t.Fatal("NewDiskStore() returned a store with nil handle")
	}
	if store.opened.Load() {
		t.Fatal("NewDiskStore() returned a store that is already marked as opened")
	}
	if store.refCount.Load() != 0 {
		t.Fatalf("NewDiskStore() returned a store with non-zero refCount, got %d", store.refCount.Load())
	}
}

func TestDiskStore_Get(t *testing.T) {
	t.Parallel()

	tempFile := setupTempDiskStore(t)
	defer os.Remove(tempFile) //nolint:errcheck,forbidigo

	store := NewDiskStore()
	store.path = tempFile

	// Test getting a non-existent key
	_, err := store.Get("non-existent")
	if err == nil {
		t.Fatal("Get() on non-existent key should return an error")
	}

	// Test getting an existing key
	expectedValue := []byte("test-value")
	err = store.Set("test-key", expectedValue)
	if err != nil {
		t.Fatalf("Failed to set up test: %v", err)
	}

	value, err := store.Get("test-key")
	if err != nil {
		t.Fatalf("Get() on existing key returned an error: %v", err)
	}

	valueBytes, ok := value.([]byte)
	if !ok {
		t.Fatalf("Get() returned a value of unexpected type, got %T, want []byte", value)
	}

	if string(valueBytes) != string(expectedValue) {
		t.Fatalf("Get() returned unexpected value, got %s, want %s", string(valueBytes), string(expectedValue))
	}

	// Clean up
	_ = store.Close()
}

func TestDiskStore_Set(t *testing.T) {
	t.Parallel()

	tempFile := setupTempDiskStore(t)
	defer os.Remove(tempFile) //nolint:errcheck,forbidigo

	store := NewDiskStore()
	store.path = tempFile
	t.Cleanup(func() {
		_ = store.Close()
	})

	// Test setting a string value
	err := store.Set("string-key", "string-value")
	if err != nil {
		t.Fatalf("Set() with string value returned an error: %v", err)
	}

	// Verify the value was stored correctly
	value, err := store.Get("string-key")
	if err != nil {
		t.Fatalf("Failed to get value after Set(): %v", err)
	}
	valueBytes, ok := value.([]byte)
	if !ok {
		t.Fatalf("Get() after Set() returned a value of unexpected type, got %T, want []byte", value)
	}
	if string(valueBytes) != "string-value" {
		t.Fatalf("Get() after Set() returned unexpected value, got %s, want %s", string(valueBytes), "string-value")
	}

	// Test setting a byte slice value
	byteValue := []byte("byte-value")
	err = store.Set("byte-key", byteValue)
	if err != nil {
		t.Fatalf("Set() with byte slice value returned an error: %v", err)
	}

	// Verify the value was stored correctly
	value, err = store.Get("byte-key")
	if err != nil {
		t.Fatalf("Failed to get value after Set(): %v", err)
	}
	valueBytes, ok = value.([]byte)
	if !ok {
		t.Fatalf("Get() after Set() returned a value of unexpected type, got %T, want []byte", value)
	}
	if string(valueBytes) != string(byteValue) {
		t.Fatalf("Get() after Set() returned unexpected value, got %s, want %s", string(valueBytes), string(byteValue))
	}

	// Test setting an unsupported value type
	err = store.Set("invalid-key", 123)
	if err == nil {
		t.Fatal("Set() with unsupported value type should return an error")
	}
}

func TestDiskStore_Delete(t *testing.T) {
	t.Parallel()

	// Create a temporary file for testing
	tempFile := setupTempDiskStore(t)
	defer os.Remove(tempFile) //nolint:errcheck,forbidigo

	store := NewDiskStore()
	store.path = tempFile
	t.Cleanup(func() {
		_ = store.Close()
	})

	// Setup
	err := store.Set("test-key", "test-value")
	if err != nil {
		t.Fatalf("Failed to set up test: %v", err)
	}

	// Test deleting an existing key
	err = store.Delete("test-key")
	if err != nil {
		t.Fatalf("Delete() returned an error: %v", err)
	}

	// Verify the key was deleted
	exists, err := store.Exists("test-key")
	if err != nil {
		t.Fatalf("Failed to check if key exists after Delete(): %v", err)
	}
	if exists {
		t.Fatal("Delete() did not remove the key from the store")
	}

	// Test deleting a non-existent key (should not error)
	err = store.Delete("non-existent")
	if err != nil {
		t.Fatalf("Delete() on non-existent key returned an error: %v", err)
	}
}

func TestDiskStore_Exists(t *testing.T) {
	t.Parallel()

	// Create a temporary file for testing
	tempFile := setupTempDiskStore(t)
	defer os.Remove(tempFile) //nolint:errcheck,forbidigo

	store := NewDiskStore()
	store.path = tempFile
	t.Cleanup(func() {
		_ = store.Close()
	})

	// Test with non-existent key
	exists, err := store.Exists("non-existent")
	if err != nil {
		t.Fatalf("Exists() returned an error: %v", err)
	}
	if exists {
		t.Fatal("Exists() returned true for a non-existent key")
	}

	// Test with existing key
	err = store.Set("test-key", "test-value")
	if err != nil {
		t.Fatalf("Failed to set up test: %v", err)
	}

	exists, err = store.Exists("test-key")
	if err != nil {
		t.Fatalf("Exists() returned an error: %v", err)
	}
	if !exists {
		t.Fatal("Exists() returned false for an existing key")
	}
}

func TestDiskStore_Clear(t *testing.T) {
	t.Parallel()

	// Create a temporary file for testing
	tempFile := setupTempDiskStore(t)
	defer os.Remove(tempFile) //nolint:errcheck,forbidigo

	store := NewDiskStore()
	store.path = tempFile
	t.Cleanup(func() {
		_ = store.Close()
	})

	// Setup
	err := store.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Failed to set up test: %v", err)
	}
	err = store.Set("key2", "value2")
	if err != nil {
		t.Fatalf("Failed to set up test: %v", err)
	}

	// Test clearing the store
	err = store.Clear()
	if err != nil {
		t.Fatalf("Clear() returned an error: %v", err)
	}

	// Verify the store is empty
	size, err := store.Size()
	if err != nil {
		t.Fatalf("Failed to get size after Clear(): %v", err)
	}
	if size != 0 {
		t.Fatalf("Clear() did not empty the store, got %d items", size)
	}
}

func TestDiskStore_Size(t *testing.T) {
	t.Parallel()

	// Create a temporary file for testing
	tempFile := setupTempDiskStore(t)
	defer os.Remove(tempFile) //nolint:errcheck,forbidigo

	store := NewDiskStore()
	store.path = tempFile
	t.Cleanup(func() {
		_ = store.Close()
	})

	// Test empty store
	size, err := store.Size()
	if err != nil {
		t.Fatalf("Size() returned an error: %v", err)
	}
	if size != 0 {
		t.Fatalf("Size() returned unexpected size for empty store, got %d, want 0", size)
	}

	// Test non-empty store
	err = store.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Failed to set up test: %v", err)
	}
	err = store.Set("key2", "value2")
	if err != nil {
		t.Fatalf("Failed to set up test: %v", err)
	}

	size, err = store.Size()
	if err != nil {
		t.Fatalf("Size() returned an error: %v", err)
	}
	if size != 2 {
		t.Fatalf("Size() returned unexpected size, got %d, want 2", size)
	}
}

func TestDiskStore_List(t *testing.T) {
	t.Parallel()

	// Create a temporary file for testing
	tempFile := setupTempDiskStore(t)
	defer os.Remove(tempFile) //nolint:errcheck,forbidigo

	store := NewDiskStore()
	store.path = tempFile
	t.Cleanup(func() {
		_ = store.Close()
	})

	// Test empty store
	entries, err := store.List("", 0)
	if err != nil {
		t.Fatalf("List() returned an error: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("List() returned unexpected entries for empty store, got %d, want 0", len(entries))
	}

	// Add some data to the store
	testData := map[string]string{
		"key1":      "value1",
		"key2":      "value2",
		"prefix1":   "value3",
		"prefix2":   "value4",
		"different": "value5",
	}

	for k, v := range testData {
		err := store.Set(k, v)
		if err != nil {
			t.Fatalf("Failed to set up test: %v", err)
		}
	}

	// Test listing all entries (no prefix, no limit)
	entries, err = store.List("", 0)
	if err != nil {
		t.Fatalf("List() returned an error: %v", err)
	}
	if len(entries) != len(testData) {
		t.Fatalf("List() returned unexpected number of entries, got %d, want %d", len(entries), len(testData))
	}

	// Verify all keys are present
	keyMap := make(map[string]bool)
	for _, entry := range entries {
		keyMap[entry.Key] = true
	}
	for k := range testData {
		if !keyMap[k] {
			t.Fatalf("List() did not return entry for key: %s", k)
		}
	}

	// Test listing with prefix
	entries, err = store.List("prefix", 0)
	if err != nil {
		t.Fatalf("List() with prefix returned an error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("List() with prefix returned unexpected number of entries, got %d, want 2", len(entries))
	}

	// Verify only entries with the prefix are returned
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Key, "prefix") {
			t.Fatalf("List() with prefix returned an entry without the prefix: %s", entry.Key)
		}
	}

	// Test listing with limit
	entries, err = store.List("", 2)
	if err != nil {
		t.Fatalf("List() with limit returned an error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("List() with limit returned unexpected number of entries, got %d, want 2", len(entries))
	}

	// Test listing with prefix and limit
	entries, err = store.List("prefix", 1)
	if err != nil {
		t.Fatalf("List() with prefix and limit returned an error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("List() with prefix and limit returned unexpected number of entries, got %d, want 1", len(entries))
	}
	if !strings.HasPrefix(entries[0].Key, "prefix") {
		t.Fatalf("List() with prefix and limit returned an entry without the prefix: %s", entries[0].Key)
	}
}

func TestDiskStore_Close(t *testing.T) {
	t.Parallel()

	// Create a temporary file for testing
	tempFile := setupTempDiskStore(t)
	defer os.Remove(tempFile) //nolint:errcheck,forbidigo

	store := NewDiskStore()
	store.path = tempFile
	t.Cleanup(func() {
		_ = store.Close()
	})

	// Open the store by performing an operation
	err := store.Set("key", "value")
	if err != nil {
		t.Fatalf("Failed to set up test: %v", err)
	}

	// Test closing the store
	err = store.Close()
	if err != nil {
		t.Fatalf("Close() returned an error: %v", err)
	}

	// Verify the store is closed
	if store.opened.Load() {
		t.Fatal("Close() did not mark the store as closed")
	}
}

func TestDiskStore_RefCount(t *testing.T) {
	t.Parallel()

	// Create a temporary file for testing
	tempFile := setupTempDiskStore(t)
	defer os.Remove(tempFile) //nolint:errcheck,forbidigo

	store := NewDiskStore()
	store.path = tempFile
	t.Cleanup(func() {
		_ = store.Close()
	})

	// Open the store by performing an operation
	err := store.Set("key", "value")
	if err != nil {
		t.Fatalf("Failed to set up test: %v", err)
	}

	// Verify the reference count is 1
	if store.refCount.Load() != 1 {
		t.Fatalf("Expected refCount to be 1 after first operation, got %d", store.refCount.Load())
	}

	// Perform another operation to increment the reference count
	_, err = store.Get("key")
	if err != nil {
		t.Fatalf("Failed to perform operation: %v", err)
	}

	// Verify the reference count is 2
	if store.refCount.Load() != 2 {
		t.Fatalf("Expected refCount to be 2 after second operation, got %d", store.refCount.Load())
	}

	// Close the store once
	err = store.Close()
	if err != nil {
		t.Fatalf("Close() returned an error: %v", err)
	}

	// Verify the reference count is 1
	if store.refCount.Load() != 1 {
		t.Fatalf("Expected refCount to be 1 after first close, got %d", store.refCount.Load())
	}

	// Verify the store is still open
	if !store.opened.Load() {
		t.Fatal("Store should still be open after first close")
	}

	// Close the store again
	err = store.Close()
	if err != nil {
		t.Fatalf("Close() returned an error: %v", err)
	}

	// Verify the reference count is 0
	if store.refCount.Load() != 0 {
		t.Fatalf("Expected refCount to be 0 after second close, got %d", store.refCount.Load())
	}

	// Verify the store is closed
	if store.opened.Load() {
		t.Fatal("Store should be closed after second close")
	}
}

// Helper function to set up a temporary disk store for testing
// seedLegacyDiskValue writes a value the way a pre-versionstamp release would:
// straight into the data bucket, with no matching entry in the versions bucket.
func seedLegacyDiskValue(t *testing.T, path, key string, value []byte) {
	t.Helper()

	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("failed to open legacy bolt db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	err = db.Update(func(tx *bolt.Tx) error {
		bucket, bucketErr := tx.CreateBucketIfNotExists([]byte(DefaultKvBucket))
		if bucketErr != nil {
			return bucketErr
		}

		return bucket.Put([]byte(key), value)
	})
	if err != nil {
		t.Fatalf("failed to seed legacy data: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close legacy bolt db: %v", err)
	}
}

// TestDiskStore_LegacyVersionstampBackfill verifies that values written by a
// pre-versionstamp release stay readable after the upgrade and are no longer
// mistaken for absent keys by an atomic absent-check.
func TestDiskStore_LegacyVersionstampBackfill(t *testing.T) {
	t.Parallel()

	tempFile := setupTempDiskStore(t)
	defer os.Remove(tempFile) //nolint:errcheck,forbidigo

	seedLegacyDiskValue(t, tempFile, "legacy", []byte(`"value"`))

	disk := NewDiskStore()
	disk.path = tempFile
	store := NewSerializedStore(disk, NewJSONSerializer())
	t.Cleanup(func() { _ = store.Close() })

	// The legacy value must remain readable after the upgrade.
	value, err := store.Get("legacy")
	if err != nil {
		t.Fatalf("Get() on legacy key returned an error: %v", err)
	}
	if value != "value" {
		t.Fatalf("Get() returned unexpected legacy value, got %#v, want %q", value, "value")
	}

	// An absent-check must not match a key that already holds a value.
	result, err := disk.AtomicCommit(
		[]Check{{Key: "legacy"}},
		[]Mutation{{Type: MutationSet, Key: "legacy", Value: []byte("overwritten")}},
	)
	if err != nil {
		t.Fatalf("AtomicCommit() returned an error: %v", err)
	}
	if result.Ok {
		t.Fatal("AtomicCommit() absent-check matched a present legacy key")
	}
}

func setupTempDiskStore(t *testing.T) string {
	// Create a temporary file
	tempFile, err := os.CreateTemp(t.TempDir(), "diskstore-test-*.db") //nolint:forbidigo
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	tempFile.Close() //nolint:errcheck

	return tempFile.Name()
}

// TestDiskStore_TableDriven demonstrates the table-driven testing approach
func TestDiskStore_TableDriven(t *testing.T) {
	t.Parallel()

	// Define test cases
	testCases := []struct {
		name      string
		setup     func(*DiskStore)
		operation func(*DiskStore) (any, error)
		validate  func(*testing.T, any, error)
		cleanup   func(*DiskStore)
	}{
		{
			name: "Set and Get string value",
			setup: func(_ *DiskStore) {
				// No setup needed
			},
			operation: func(s *DiskStore) (any, error) {
				err := s.Set("key", "value")
				if err != nil {
					return nil, err
				}
				return s.Get("key")
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				bytes, ok := result.([]byte)
				if !ok {
					t.Fatalf("Expected []byte, got %T", result)
				}

				if string(bytes) != "value" {
					t.Fatalf("Expected 'value', got '%s'", string(bytes))
				}
			},
			cleanup: func(s *DiskStore) {
				_ = s.Close()
			},
		},
		{
			name: "Get non-existent key",
			setup: func(_ *DiskStore) {
				// No setup needed
			},
			operation: func(s *DiskStore) (any, error) {
				return s.Get("non-existent")
			},
			validate: func(t *testing.T, _ any, err error) {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
			},
			cleanup: func(s *DiskStore) {
				_ = s.Close()
			},
		},
		{
			name: "Delete existing key",
			setup: func(s *DiskStore) {
				_ = s.Set("key", "value")
			},
			operation: func(s *DiskStore) (any, error) {
				err := s.Delete("key")
				if err != nil {
					return nil, err
				}

				exists, err := s.Exists("key")
				return exists, err
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				exists, ok := result.(bool)
				if !ok {
					t.Fatalf("Expected bool, got %T", result)
				}

				if exists {
					t.Fatal("Key should not exist after deletion")
				}
			},
			cleanup: func(s *DiskStore) {
				_ = s.Close()
			},
		},
		{
			name: "Clear store",
			setup: func(s *DiskStore) {
				_ = s.Set("key1", "value1")
				_ = s.Set("key2", "value2")
			},
			operation: func(s *DiskStore) (any, error) {
				err := s.Clear()
				if err != nil {
					return nil, err
				}

				return s.Size()
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				size, ok := result.(int64)
				if !ok {
					t.Fatalf("Expected int64, got %T", result)
				}

				if size != 0 {
					t.Fatalf("Expected size 0, got %d", size)
				}
			},
			cleanup: func(s *DiskStore) {
				_ = s.Close()
			},
		},
		{
			name: "Reference counting",
			setup: func(_ *DiskStore) {
				// No setup needed
			},
			operation: func(s *DiskStore) (any, error) {
				// Perform operations to increment reference count
				err := s.Set("key", "value")
				if err != nil {
					return nil, err
				}

				_, err = s.Get("key")
				if err != nil {
					return nil, err
				}

				// Close once to decrement reference count
				err = s.Close()
				if err != nil {
					return nil, err
				}

				// Store should still be open
				return s.opened.Load(), nil
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				opened, ok := result.(bool)
				if !ok {
					t.Fatalf("Expected bool, got %T", result)
				}

				if !opened {
					t.Fatal("Store should still be open after first close")
				}
			},
			cleanup: func(s *DiskStore) {
				// Close again to fully close the store
				_ = s.Close()
			},
		},
		{
			name: "List entries with prefix",
			setup: func(s *DiskStore) {
				_ = s.Set("prefix1", "value1")
				_ = s.Set("prefix2", "value2")
				_ = s.Set("other", "value3")
			},
			operation: func(s *DiskStore) (any, error) {
				return s.List("prefix", 0)
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				entries, ok := result.([]Entry)
				if !ok {
					t.Fatalf("Expected []Entry, got %T", result)
				}

				if len(entries) != 2 {
					t.Fatalf("Expected 2 entries, got %d", len(entries))
				}

				// Verify all entries have the prefix
				for _, entry := range entries {
					if !strings.HasPrefix(entry.Key, "prefix") {
						t.Fatalf("Entry key %s does not have prefix 'prefix'", entry.Key)
					}
				}
			},
			cleanup: func(s *DiskStore) {
				_ = s.Close()
			},
		},
		{
			name: "List entries with limit",
			setup: func(s *DiskStore) {
				_ = s.Set("key1", "value1")
				_ = s.Set("key2", "value2")
				_ = s.Set("key3", "value3")
			},
			operation: func(s *DiskStore) (any, error) {
				return s.List("", 2)
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				entries, ok := result.([]Entry)
				if !ok {
					t.Fatalf("Expected []Entry, got %T", result)
				}

				if len(entries) != 2 {
					t.Fatalf("Expected 2 entries, got %d", len(entries))
				}
			},
			cleanup: func(s *DiskStore) {
				_ = s.Close()
			},
		},
		{
			name: "List entries with prefix and limit",
			setup: func(s *DiskStore) {
				_ = s.Set("prefix1", "value1")
				_ = s.Set("prefix2", "value2")
				_ = s.Set("other", "value3")
			},
			operation: func(s *DiskStore) (any, error) {
				return s.List("prefix", 1)
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				entries, ok := result.([]Entry)
				if !ok {
					t.Fatalf("Expected []Entry, got %T", result)
				}

				if len(entries) != 1 {
					t.Fatalf("Expected 1 entry, got %d", len(entries))
				}

				if !strings.HasPrefix(entries[0].Key, "prefix") {
					t.Fatalf("Entry key %s does not have prefix 'prefix'", entries[0].Key)
				}
			},
			cleanup: func(s *DiskStore) {
				_ = s.Close()
			},
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create a temporary file for testing
			tempFile := setupTempDiskStore(t)
			defer os.Remove(tempFile) //nolint:errcheck,forbidigo

			store := NewDiskStore()
			store.path = tempFile
			t.Cleanup(func() {
				_ = store.Close()
			})

			tc.setup(store)
			result, err := tc.operation(store)
			tc.validate(t, result, err)
			tc.cleanup(store)
		})
	}
}

func newTempDiskStore(t *testing.T) *DiskStore {
	t.Helper()

	// setupTempDiskStore places the file under t.TempDir(), which is removed
	// automatically when the test finishes, so no explicit cleanup is needed.
	store := NewDiskStore()
	store.path = setupTempDiskStore(t)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

func TestDiskStore_GetEntry(t *testing.T) {
	t.Parallel()

	store := newTempDiskStore(t)

	// Absent key: Found is false, with a nil value and empty versionstamp.
	absent, err := store.GetEntry("missing")
	if err != nil {
		t.Fatalf("GetEntry() on a missing key returned an error: %v", err)
	}
	if absent.Found {
		t.Fatal("GetEntry() reported a missing key as present")
	}
	if absent.Value != nil {
		t.Fatalf("GetEntry() on a missing key returned a non-nil value: %#v", absent.Value)
	}
	if absent.Versionstamp != "" {
		t.Fatalf("GetEntry() on a missing key returned a versionstamp: %q", absent.Versionstamp)
	}

	// Present key: Found is true, with the stored value and a versionstamp.
	if err := store.Set("present", []byte("value")); err != nil {
		t.Fatalf("Set() returned an error: %v", err)
	}
	entry, err := store.GetEntry("present")
	if err != nil {
		t.Fatalf("GetEntry() on a present key returned an error: %v", err)
	}
	if !entry.Found {
		t.Fatal("GetEntry() reported a present key as missing")
	}
	if string(entry.Value.([]byte)) != "value" {
		t.Fatalf("GetEntry() returned unexpected value, got %#v, want %q", entry.Value, "value")
	}
	if entry.Versionstamp == "" {
		t.Fatal("GetEntry() returned an empty versionstamp for a present key")
	}
}

func TestDiskStore_GetMany(t *testing.T) {
	t.Parallel()

	store := newTempDiskStore(t)

	if err := store.Set("a", []byte("1")); err != nil {
		t.Fatalf("Set() returned an error: %v", err)
	}
	if err := store.Set("c", []byte("3")); err != nil {
		t.Fatalf("Set() returned an error: %v", err)
	}

	// Order must match the input keys, with a placeholder for the absent key.
	entries, err := store.GetMany([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("GetMany() returned an error: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("GetMany() returned %d entries, want 3", len(entries))
	}

	wantKeys := []string{"a", "b", "c"}
	for i, want := range wantKeys {
		if entries[i].Key != want {
			t.Fatalf("GetMany() entry %d has key %q, want %q", i, entries[i].Key, want)
		}
	}

	if !entries[0].Found || string(entries[0].Value.([]byte)) != "1" {
		t.Fatalf("GetMany() entry for %q is wrong: %+v", "a", entries[0])
	}
	if entries[1].Found {
		t.Fatalf("GetMany() reported absent key %q as present: %+v", "b", entries[1])
	}
	if !entries[2].Found || string(entries[2].Value.([]byte)) != "3" {
		t.Fatalf("GetMany() entry for %q is wrong: %+v", "c", entries[2])
	}
}

func TestDiskStore_VersionstampMonotonic(t *testing.T) {
	t.Parallel()

	store := newTempDiskStore(t)

	if err := store.Set("k", []byte("v1")); err != nil {
		t.Fatalf("Set() returned an error: %v", err)
	}
	first, err := store.GetEntry("k")
	if err != nil {
		t.Fatalf("GetEntry() returned an error: %v", err)
	}

	if err := store.Set("k", []byte("v2")); err != nil {
		t.Fatalf("Set() returned an error: %v", err)
	}
	second, err := store.GetEntry("k")
	if err != nil {
		t.Fatalf("GetEntry() returned an error: %v", err)
	}

	// Versionstamps are fixed-width, so lexical order matches write order.
	if second.Versionstamp <= first.Versionstamp {
		t.Fatalf("versionstamp did not advance: first=%q second=%q", first.Versionstamp, second.Versionstamp)
	}
}

func TestDiskStore_AtomicCommit(t *testing.T) {
	t.Parallel()

	store := newTempDiskStore(t)

	// A commit whose checks all hold applies its mutations and reports a versionstamp.
	result, err := store.AtomicCommit(
		[]Check{{Key: "k"}}, // expect "k" absent
		[]Mutation{{Type: MutationSet, Key: "k", Value: []byte("v")}},
	)
	if err != nil {
		t.Fatalf("AtomicCommit() returned an error: %v", err)
	}
	if !result.Ok {
		t.Fatal("AtomicCommit() with a satisfied check did not commit")
	}
	if result.Versionstamp == "" {
		t.Fatal("AtomicCommit() committed without returning a versionstamp")
	}

	entry, err := store.GetEntry("k")
	if err != nil {
		t.Fatalf("GetEntry() returned an error: %v", err)
	}
	if !entry.Found || string(entry.Value.([]byte)) != "v" {
		t.Fatalf("AtomicCommit() did not persist the mutation: %+v", entry)
	}
	if entry.Versionstamp != result.Versionstamp {
		t.Fatalf("stored versionstamp %q does not match commit result %q", entry.Versionstamp, result.Versionstamp)
	}

	// A commit whose check no longer holds is rejected and changes nothing.
	rejected, err := store.AtomicCommit(
		[]Check{{Key: "k"}}, // expect "k" absent, but it now holds "v"
		[]Mutation{{Type: MutationSet, Key: "k", Value: []byte("other")}},
	)
	if err != nil {
		t.Fatalf("AtomicCommit() returned an error: %v", err)
	}
	if rejected.Ok {
		t.Fatal("AtomicCommit() applied mutations despite a failed check")
	}

	unchanged, err := store.GetEntry("k")
	if err != nil {
		t.Fatalf("GetEntry() returned an error: %v", err)
	}
	if string(unchanged.Value.([]byte)) != "v" {
		t.Fatalf("rejected commit mutated the store: %+v", unchanged)
	}

	// A delete mutation behind a matching versionstamp check removes the key.
	deleted, err := store.AtomicCommit(
		[]Check{{Key: "k", Versionstamp: unchanged.Versionstamp}},
		[]Mutation{{Type: MutationDelete, Key: "k"}},
	)
	if err != nil {
		t.Fatalf("AtomicCommit() returned an error: %v", err)
	}
	if !deleted.Ok {
		t.Fatal("AtomicCommit() with a matching versionstamp check did not commit")
	}

	gone, err := store.GetEntry("k")
	if err != nil {
		t.Fatalf("GetEntry() returned an error: %v", err)
	}
	if gone.Found {
		t.Fatal("AtomicCommit() delete mutation left the key in place")
	}
}

func TestDiskStore_AtomicCommitConcurrentAbsentCheck(t *testing.T) {
	t.Parallel()

	store := newTempDiskStore(t)

	const goroutines = 20
	results := make(chan CommitResult, goroutines)
	for range goroutines {
		go func() {
			result, err := store.AtomicCommit(
				[]Check{{Key: "lock"}},
				[]Mutation{{Type: MutationSet, Key: "lock", Value: []byte("owner")}},
			)
			if err != nil {
				t.Errorf("AtomicCommit() returned error: %v", err)
				return
			}
			results <- result
		}()
	}

	var wins int
	for range goroutines {
		if result := <-results; result.Ok {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("AtomicCommit() allowed %d absent-check winners, want 1", wins)
	}
}
