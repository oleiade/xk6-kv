package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/grafana/sobek"
	bolt "go.etcd.io/bbolt"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/js/promises"
)

// KV is a key-value database that can be used to store and retrieve data.
//
// Keys are always strings, and values can be any JSON-serializable value.
// Keys are ordered lexicographically.
// Keys are unique within a database, and the last value set for a given key is the one that
// is returned when reading the key.
type KV struct {
	// bucket is the name of the BoltDB bucket that this KV instance uses.
	bucket []byte

	// db is the BoltDB instance that this KV instance uses.
	// db *bolt.DB
	db *db

	// vu is the VU instance that this KV instance belongs to.
	vu modules.VU
}

// NewKV returns a new KV instance.
func NewKV(vu modules.VU, db *db) *KV {
	return &KV{
		bucket: []byte(DefaultKvBucket),
		vu:     vu,
		db:     db,
	}
}

// Set sets the value of a key in the store.
//
// If the key does not exist, it is created. If the key already exists, its value is overwritten.
func (k *KV) Set(key sobek.Value, value sobek.Value) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	// Convert the key to a byte slice
	keyBytes, err := common.ToBytes(key.Export())
	if err != nil {
		reject(err)
		return promise
	}

	jsonValue, err := json.Marshal(value.Export())
	if err != nil {
		reject(err)
		return promise
	}

	go func() {
		// Update the value in the database within a BoltDB transaction
		err := k.db.handle.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(k.bucket)
			if bucket == nil {
				return fmt.Errorf("bucket not found")
			}

			return bucket.Put(keyBytes, jsonValue)
		})
		if err != nil {
			reject(err)
			return
		}

		resolve(value)
	}()

	return promise
}

// Get returns the value of a key in the store.
func (k *KV) Get(key sobek.Value) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	// Convert the key to a byte slice
	keyBytes, err := common.ToBytes(key.Export())
	if err != nil {
		reject(err)
		return promise
	}

	go func() {
		var jsonValue []byte

		// Get the value from the database within a BoltDB transaction
		err := k.db.handle.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(k.bucket)
			if bucket == nil {
				return fmt.Errorf("bucket not found")
			}

			jsonValue = bucket.Get(keyBytes)

			return nil
		})
		if err != nil {
			reject(err)
			return
		}

		if jsonValue == nil {
			reject(NewError(KeyNotFoundError, "key "+key.String()+" not found"))
			return
		}

		var value any
		if err := json.Unmarshal(jsonValue, &value); err != nil {
			reject(err)
			return
		}

		resolve(k.vu.Runtime().ToValue(value))
	}()

	return promise
}

// Delete deletes a key from the store.
func (k *KV) Delete(key sobek.Value) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	keyBytes, err := common.ToBytes(key.Export())
	if err != nil {
		reject(err)
		return promise
	}

	go func() {
		err := k.db.handle.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(k.bucket)
			if bucket == nil {
				return NewError(BucketNotFoundError, "bucket "+string(k.bucket)+" not found")
			}

			return bucket.Delete(keyBytes)
		})
		if err != nil {
			reject(err)
			return
		}

		resolve(true)
	}()

	return promise
}

// List returns all the key-value pairs in the store.
//
// The returned list is ordered lexicographically by key.
// The returned list is limited to 1000 entries by default.
// The returned list can be limited to a maximum number of entries by passing a limit option.
// The returned list can be limited to keys that start with a given prefix by passing a prefix option.
// See [ListOptions] for more details
func (k *KV) List(options sobek.Value) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	listOptions := ImportListOptions(k.vu.Runtime(), options)

	go func() {
		var entries []ListEntry

		err := k.db.handle.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(k.bucket)
			if bucket == nil {
				return NewError(BucketNotFoundError, "bucket "+string(k.bucket)+" not found")
			}

			var listed int64
			return bucket.ForEach(func(k, v []byte) error {
				if listOptions.limitSet && listed >= listOptions.Limit {
					return ErrStop
				}

				key := string(k)

				if !strings.HasPrefix(key, listOptions.Prefix) {
					return nil
				}

				var value any
				if err := json.Unmarshal(v, &value); err != nil {
					return err
				}

				entries = append(entries, ListEntry{key, value})
				listed++

				return nil
			})
		})
		if err != nil && !errors.Is(err, ErrStop) {
			reject(err)
			return
		}

		resolve(k.vu.Runtime().ToValue(entries))
	}()

	return promise
}

// ListEntry is a key-value pair returned by KV.List().
type ListEntry struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// ListOptions are the options that can be passed to KV.List().
type ListOptions struct {
	// Prefix is used to select all the keys that start
	// with the given prefix.
	Prefix string `json:"prefix"`

	// Limit is the maximum number of entries to return.
	Limit int64 `json:"limit"`

	limitSet bool
}

// ErrStop is used to stop a BoltDB iteration.
var ErrStop = errors.New("stop")

// ImportListOptions instantiates a ListOptions from a sobek.Value.
func ImportListOptions(rt *sobek.Runtime, options sobek.Value) ListOptions {
	listOptions := ListOptions{}

	// If no options are passed, return the default options
	if common.IsNullish(options) {
		return listOptions
	}

	// Interpret the options as an object
	optionsObj := options.ToObject(rt)

	listOptions.Prefix = optionsObj.Get("prefix").String()

	limitValue := optionsObj.Get("limit")
	if limitValue == nil {
		return listOptions
	}

	var limit int64
	err := rt.ExportTo(limitValue, &limit)
	if err == nil {
		listOptions.Limit = limit
		listOptions.limitSet = true
	}

	return listOptions
}

// Clear deletes all the keys in the store.
func (k *KV) Clear() *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	go func() {
		err := k.db.handle.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(k.bucket)
			if bucket == nil {
				return NewError(BucketNotFoundError, "bucket "+string(k.bucket)+" not found")
			}

			return bucket.ForEach(func(k, _ []byte) error {
				return bucket.Delete(k)
			})
		})
		if err != nil {
			reject(err)
			return
		}

		resolve(true)
	}()

	return promise
}

// Size returns the number of keys in the store.
func (k *KV) Size() *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	go func() {
		var size int64

		err := k.db.handle.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(k.bucket)
			if bucket == nil {
				return NewError(BucketNotFoundError, "bucket "+string(k.bucket)+" not found")
			}

			size = int64(bucket.Stats().KeyN)

			return nil
		})
		if err != nil {
			reject(err)
			return
		}

		resolve(size)
	}()

	return promise
}

// Close closes the KV instance.
func (k *KV) Close() error {
	return k.db.close()
}
