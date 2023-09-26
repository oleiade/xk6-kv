package kv

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

//nolint:forbidigo
func TestDbOpen(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "kvtest")
	require.NoError(t, err)
	t.Cleanup(func() {
		removeErr := os.RemoveAll(tmpDir)
		require.NoError(t, removeErr)
	})

	t.Run("calling open on a new db instance successfully opens the database", func(t *testing.T) {
		t.Parallel()

		// Create a new db instance and
		// override the default path for testing purposes
		dbInstance := newDB()
		dbInstance.path = filepath.Join(tmpDir, randomFileName("test.", ".db"))

		// Open the database
		gotErr := dbInstance.open()
		t.Cleanup(func() {
			require.NoError(t, gotErr)
			require.NoError(t, dbInstance.close())
		})

		// Test that calling open on a new db instance successfully opens the database.
		assert.NoError(t, gotErr)
		assert.True(t, dbInstance.opened.Load())
		assert.Equal(t, int64(1), dbInstance.refCount.Load())
	})

	t.Run("calling open on an already open db instance increments the ref count", func(t *testing.T) {
		t.Parallel()

		// Create a new db instance and
		// override the default path for testing purposes
		dbInstance := newDB()
		dbInstance.path = filepath.Join(tmpDir, randomFileName("test.", ".db"))

		// Pre-open the database
		require.NoError(t, dbInstance.open())
		t.Cleanup(func() {
			require.NoError(t, dbInstance.close())
		})

		gotErr := dbInstance.open()
		t.Cleanup(func() {
			require.NoError(t, gotErr)
			require.NoError(t, dbInstance.close())
		})

		// Test that calling open on an already open db instance increments the ref count.
		assert.NoError(t, gotErr)
		assert.True(t, dbInstance.opened.Load())
		assert.Equal(t, int64(2), dbInstance.refCount.Load())
	})

	t.Run("ensure the default bucket is created if it doesn't exist", func(t *testing.T) {
		t.Parallel()

		// Create a new db instance and
		// override the default path for testing purposes
		dbInstance := newDB()
		dbInstance.path = filepath.Join(tmpDir, randomFileName("test.", ".db"))

		// Open the database
		require.NoError(t, dbInstance.open())
		t.Cleanup(func() {
			require.NoError(t, dbInstance.close())
		})

		// Ensure that the default bucket is created if it doesn't exist.
		assert.NoError(t, dbInstance.handle.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(DefaultKvBucket))
			assert.NotNil(t, bucket)
			return nil
		}))
	})
}

//nolint:forbidigo
func TestDbClose(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "kvtest")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(tmpDir))
	})

	t.Run("1", func(t *testing.T) {
		t.Parallel()

		// Initialize a new db instance and open it
		dbInstance := newDB()
		dbInstance.path = filepath.Join(tmpDir, randomFileName("test.", ".db"))
		require.NoError(t, dbInstance.open())

		gotErr := dbInstance.close()

		assert.NoError(t, gotErr)
		assert.Equal(t, int64(0), dbInstance.refCount.Load())
		assert.False(t, dbInstance.opened.Load())
		assert.Nil(t, dbInstance.handle)
	})

	t.Run("closing a db with non-zero ref count should not actually close", func(t *testing.T) {
		t.Parallel()

		// Initialize a new db instance and open it
		dbInstance := newDB()
		dbInstance.path = filepath.Join(tmpDir, randomFileName("test.", ".db"))

		// Pre-open the database twice, so the ref count is 2
		require.NoError(t, dbInstance.open())
		require.NoError(t, dbInstance.open())

		gotErr := dbInstance.close()

		assert.NoError(t, gotErr)
		assert.Equal(t, int64(1), dbInstance.refCount.Load())
		assert.True(t, dbInstance.opened.Load())
		assert.NotNil(t, dbInstance.handle)

		t.Cleanup(func() {
			require.NoError(t, dbInstance.close())
		})
	})
}

//nolint:unparam
func randomFileName(prefix, suffix string) string {
	return prefix + fmt.Sprint(rand.Intn(100)) + suffix //nolint:gosec
}
