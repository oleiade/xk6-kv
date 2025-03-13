package store

import (
	"strings"
	"testing"
)

func TestNewMemoryStore(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	if store == nil {
		t.Fatal("NewMemoryStore() returned nil")
	}
	if store.container == nil {
		t.Fatal("NewMemoryStore() returned a store with nil container")
	}
	if len(store.container) != 0 {
		t.Fatalf("NewMemoryStore() returned a store with non-empty container, got %d items", len(store.container))
	}
}

func TestMemoryStore_Get(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()

	// Test getting a non-existent key
	_, err := store.Get("non-existent")
	if err == nil {
		t.Fatal("Get() on non-existent key should return an error")
	}

	// Test getting an existing key
	expectedValue := []byte("test-value")
	store.container["test-key"] = expectedValue

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
}

func TestMemoryStore_Set(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()

	// Test setting a string value
	err := store.Set("string-key", "string-value")
	if err != nil {
		t.Fatalf("Set() with string value returned an error: %v", err)
	}

	value, exists := store.container["string-key"]
	if !exists {
		t.Fatal("Set() with string value did not store the key")
	}
	if string(value) != "string-value" {
		t.Fatalf("Set() with string value stored unexpected value, got %s, want %s", string(value), "string-value")
	}

	// Test setting a byte slice value
	byteValue := []byte("byte-value")
	err = store.Set("byte-key", byteValue)
	if err != nil {
		t.Fatalf("Set() with byte slice value returned an error: %v", err)
	}

	value, exists = store.container["byte-key"]
	if !exists {
		t.Fatal("Set() with byte slice value did not store the key")
	}
	if string(value) != string(byteValue) {
		t.Fatalf("Set() with byte slice value stored unexpected value, got %s, want %s", string(value), string(byteValue))
	}

	// Test setting an unsupported value type
	err = store.Set("invalid-key", 123)
	if err == nil {
		t.Fatal("Set() with unsupported value type should return an error")
	}
}

func TestMemoryStore_Delete(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()

	// Setup
	store.container["test-key"] = []byte("test-value")

	// Test deleting an existing key
	err := store.Delete("test-key")
	if err != nil {
		t.Fatalf("Delete() returned an error: %v", err)
	}

	_, exists := store.container["test-key"]
	if exists {
		t.Fatal("Delete() did not remove the key from the container")
	}

	// Test deleting a non-existent key (should not error)
	err = store.Delete("non-existent")
	if err != nil {
		t.Fatalf("Delete() on non-existent key returned an error: %v", err)
	}
}

func TestMemoryStore_Exists(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()

	// Test with non-existent key
	exists, err := store.Exists("non-existent")
	if err != nil {
		t.Fatalf("Exists() returned an error: %v", err)
	}
	if exists {
		t.Fatal("Exists() returned true for a non-existent key")
	}

	// Test with existing key
	store.container["test-key"] = []byte("test-value")

	exists, err = store.Exists("test-key")
	if err != nil {
		t.Fatalf("Exists() returned an error: %v", err)
	}
	if !exists {
		t.Fatal("Exists() returned false for an existing key")
	}
}

func TestMemoryStore_Clear(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()

	// Setup
	store.container["key1"] = []byte("value1")
	store.container["key2"] = []byte("value2")

	// Test clearing the store
	err := store.Clear()
	if err != nil {
		t.Fatalf("Clear() returned an error: %v", err)
	}

	if len(store.container) != 0 {
		t.Fatalf("Clear() did not empty the container, got %d items", len(store.container))
	}
}

func TestMemoryStore_Size(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()

	// Test empty store
	size, err := store.Size()
	if err != nil {
		t.Fatalf("Size() returned an error: %v", err)
	}
	if size != 0 {
		t.Fatalf("Size() returned unexpected size for empty store, got %d, want 0", size)
	}

	// Test non-empty store
	store.container["key1"] = []byte("value1")
	store.container["key2"] = []byte("value2")

	size, err = store.Size()
	if err != nil {
		t.Fatalf("Size() returned an error: %v", err)
	}
	if size != 2 {
		t.Fatalf("Size() returned unexpected size, got %d, want 2", size)
	}
}

func TestMemoryStore_List(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()

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

	// Verify entries are sorted by key
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Key > entries[i].Key {
			t.Fatalf("List() returned entries in wrong order, %s should come before %s", entries[i].Key, entries[i-1].Key)
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

func TestMemoryStore_Close(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()

	// Close should be a no-op for MemoryStore
	err := store.Close()
	if err != nil {
		t.Fatalf("Close() returned an error: %v", err)
	}
}

func TestMemoryStore_Concurrency(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	done := make(chan bool)

	// Test concurrent reads and writes
	go func() {
		for range 100 {
			store.Set("key", "value") //nolint:errcheck,gosec
		}
		done <- true
	}()

	go func() {
		for range 100 {
			store.Get("key") //nolint:errcheck,gosec
		}
		done <- true
	}()

	// Wait for both goroutines to finish
	<-done
	<-done

	// If we got here without deadlocking, the test passes
}

// TestMemoryStore_TableDemonstrates demonstrates the table-driven testing approach
func TestMemoryStore_TableDriven(t *testing.T) {
	t.Parallel()

	// Define test cases
	testCases := []struct {
		name      string
		setup     func(*MemoryStore)
		operation func(*MemoryStore) (any, error)
		validate  func(*testing.T, any, error)
	}{
		{
			name: "Clear store",
			setup: func(s *MemoryStore) {
				s.container["key1"] = []byte("value1")
				s.container["key2"] = []byte("value2")
			},
			operation: func(s *MemoryStore) (any, error) {
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
		},
		{
			name: "List entries with prefix",
			setup: func(s *MemoryStore) {
				s.container["prefix1"] = []byte("value1")
				s.container["prefix2"] = []byte("value2")
				s.container["other"] = []byte("value3")
			},
			operation: func(s *MemoryStore) (any, error) {
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

				// Verify entries are sorted
				if entries[0].Key > entries[1].Key {
					t.Fatalf("Entries are not sorted, got %s before %s", entries[0].Key, entries[1].Key)
				}
			},
		},
		{
			name: "List entries with limit",
			setup: func(s *MemoryStore) {
				s.container["key1"] = []byte("value1")
				s.container["key2"] = []byte("value2")
				s.container["key3"] = []byte("value3")
			},
			operation: func(s *MemoryStore) (any, error) {
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
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore()
			tc.setup(store)
			result, err := tc.operation(store)
			tc.validate(t, result, err)
		})
	}
}
