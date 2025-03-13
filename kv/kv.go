package kv

import (
	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/js/promises"

	"github.com/oleiade/xk6-kv/kv/store"
)

// KV is a key-value database that can be used to store and retrieve data.
//
// Keys are always strings, and values can be any JSON-serializable value.
// Keys are ordered lexicographically.
// Keys are unique within a database, and the last value set for a given key is the one that
// is returned when reading the key.
type KV struct {
	// store is the Store instance that this KV instance uses.
	store store.Store

	// vu is the VU instance that this KV instance belongs to.
	vu modules.VU
}

// NewKV returns a new KV instance.
func NewKV(vu modules.VU, s store.Store) *KV {
	return &KV{
		vu:    vu,
		store: s,
	}
}

// Set sets the value of a key in the store.
//
// If the key does not exist, it is created. If the key already exists, its value is overwritten.
func (k *KV) Set(key sobek.Value, value sobek.Value) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	keyString := key.String()
	exportedValue := value.Export()

	go func() {
		if k.store == nil {
			reject(NewError(DatabaseNotOpenError, "database is not open"))
			return
		}

		err := k.store.Set(keyString, exportedValue)
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

	keyString := key.String()

	go func() {
		if k.store == nil {
			reject(NewError(DatabaseNotOpenError, "database is not open"))
			return
		}

		value, err := k.store.Get(keyString)
		if err != nil {
			reject(err)
			return
		}

		// Convert the value to a JavaScript value
		jsValue := k.vu.Runtime().ToValue(value)
		resolve(jsValue)
	}()

	return promise
}

// Delete deletes a key from the store.
func (k *KV) Delete(key sobek.Value) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	keyString := key.String()

	go func() {
		if k.store == nil {
			reject(NewError(DatabaseNotOpenError, "database is not open"))
			return
		}

		err := k.store.Delete(keyString)
		if err != nil {
			reject(err)
			return
		}

		resolve(true)
	}()

	return promise
}

// Exists checks if a given key exists.
func (k *KV) Exists(key sobek.Value) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	keyString := key.String()

	go func() {
		if k.store == nil {
			reject(NewError(DatabaseNotOpenError, "database is not open"))
			return
		}

		exists, err := k.store.Exists(keyString)
		if err != nil {
			reject(err)
			return
		}

		resolve(exists)
	}()

	return promise
}

// Clear deletes all the keys in the store.
func (k *KV) Clear() *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	go func() {
		if k.store == nil {
			reject(NewError(DatabaseNotOpenError, "database is not open"))
			return
		}

		err := k.store.Clear()
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
		if k.store == nil {
			reject(NewError(DatabaseNotOpenError, "database is not open"))
			return
		}

		size, err := k.store.Size()
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
	return k.store.Close()
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

	// Import list options from JavaScript
	listOptions := ImportListOptions(k.vu.Runtime(), options)

	go func() {
		if k.store == nil {
			reject(NewError(DatabaseNotOpenError, "database is not open"))
			return
		}

		// Use the store interface to list entries
		entries, err := k.store.List(listOptions.Prefix, listOptions.Limit)
		if err != nil {
			reject(err)
			return
		}

		// Convert entries to ListEntry format for JavaScript
		jsEntries := make([]ListEntry, len(entries))
		for i, entry := range entries {
			jsEntries[i] = ListEntry{
				Key:   entry.Key,
				Value: entry.Value,
			}
		}

		resolve(k.vu.Runtime().ToValue(jsEntries))
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
