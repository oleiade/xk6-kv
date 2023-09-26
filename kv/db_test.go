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

	// Create a new db instance and
	// override the default path for testing purposes
	dbInstance := newDB()
	dbInstance.path = filepath.Join(tmpDir, randomFileName("test.", ".db"))

	// Test that calling open on a new db instance successfully opens the database.
	assert.NoError(t, dbInstance.open())
	assert.True(t, dbInstance.opened.Load())
	assert.Equal(t, int64(1), dbInstance.refCount.Load())

	// Test that calling open on an already open db instance increments the ref count.
	require.NoError(t, dbInstance.open())
	require.True(t, dbInstance.opened.Load())
	require.Equal(t, int64(2), dbInstance.refCount.Load())

	// Ensure that the default bucket is created if it doesn't exist.
	require.NoError(t, dbInstance.handle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultKvBucket))
		require.NotNil(t, bucket)
		return nil
	}))
}

//nolint:forbidigo
func TestDbClose(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "kvtest")
	require.NoError(t, err)
	t.Cleanup(func() {
		removeErr := os.RemoveAll(tmpDir)
		require.NoError(t, removeErr)
	})

	// Initialize a new db instance and open it
	dbInstance := newDB()
	dbInstance.path = filepath.Join(tmpDir, randomFileName("test.", ".db"))
	require.NoError(t, dbInstance.open())

	// 3. If the reference count is more than one and close is called, the database should not actually close.
	require.NoError(t, dbInstance.open())
	require.Equal(t, int64(2), dbInstance.refCount.Load())
	require.NoError(t, dbInstance.close())
	require.True(t, dbInstance.opened.Load())
	require.Equal(t, int64(1), dbInstance.refCount.Load())

	// 4. If the reference count is one and close is called, the database should close.
	require.NoError(t, dbInstance.close())
	require.False(t, dbInstance.opened.Load())
	require.Equal(t, int64(0), dbInstance.refCount.Load())
}

func randomFileName(prefix, suffix string) string {
	return prefix + fmt.Sprint(rand.Intn(100)) + suffix //nolint:gosec
}
