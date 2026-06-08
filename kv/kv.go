package kv

import (
	"errors"

	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/common"
	"go.k6.io/k6/v2/js/modules"
	"go.k6.io/k6/v2/js/promises"

	"github.com/oleiade/xk6-kv/kv/store"
)

// KV is a key-value database that can be used to store and retrieve data.
//
// Keys are always strings, and values can be any JSON-serializable value.
// Keys are ordered lexicographically.
// Keys are unique within a database, and the last value set for a given key is the one that
// is returned when reading the key.
type KV struct {
	// store is the Store instance that this KV instance uses. It is set once
	// by NewKV (which is only called from OpenKv after the shared store has
	// been constructed) and is never nil for any KV reachable from JS.
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

// async runs work on a goroutine and resolves/rejects the resulting Promise
// from the JS event loop. It is the only place this package creates a
// Promise; each public KV method becomes a thin wrapper that captures any
// sobek-side values up front and then hands a closure to async.
func (k *KV) async(fn func() (any, error)) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)
	go func() {
		value, err := fn()
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				reject(NewError(KeyNotFoundError, err.Error()))
				return
			}
			reject(err)
			return
		}
		resolve(value)
	}()
	return promise
}

// Set sets the value of a key in the store.
//
// If the key does not exist, it is created. If the key already exists, its value is overwritten.
func (k *KV) Set(key sobek.Value, value sobek.Value) *sobek.Promise {
	keyString := key.String()
	exportedValue := value.Export()
	return k.async(func() (any, error) {
		if err := k.store.Set(keyString, exportedValue); err != nil {
			return nil, err
		}
		return value, nil
	})
}

// Get returns the value of a key in the store.
func (k *KV) Get(key sobek.Value) *sobek.Promise {
	keyString := key.String()
	return k.async(func() (any, error) {
		value, err := k.store.Get(keyString)
		if err != nil {
			return nil, err
		}
		return k.vu.Runtime().ToValue(value), nil
	})
}

// Delete deletes a key from the store.
func (k *KV) Delete(key sobek.Value) *sobek.Promise {
	keyString := key.String()
	return k.async(func() (any, error) {
		if err := k.store.Delete(keyString); err != nil {
			return nil, err
		}
		return true, nil
	})
}

// Exists checks if a given key exists.
func (k *KV) Exists(key sobek.Value) *sobek.Promise {
	keyString := key.String()
	return k.async(func() (any, error) {
		return k.store.Exists(keyString)
	})
}

// Clear deletes all the keys in the store.
func (k *KV) Clear() *sobek.Promise {
	return k.async(func() (any, error) {
		if err := k.store.Clear(); err != nil {
			return nil, err
		}
		return true, nil
	})
}

// Size returns the number of keys in the store.
func (k *KV) Size() *sobek.Promise {
	return k.async(func() (any, error) {
		return k.store.Size()
	})
}

// Close closes the KV instance.
func (k *KV) Close() error {
	return k.store.Close()
}

// List returns all the key-value pairs in the store.
//
// The returned list is ordered lexicographically by key.
// The returned list can be limited to a maximum number of entries by passing a limit option.
// The returned list can be limited to keys that start with a given prefix by passing a prefix option.
// See [ListOptions] for more details.
func (k *KV) List(options sobek.Value) *sobek.Promise {
	listOptions := ImportListOptions(k.vu.Runtime(), options)
	return k.async(func() (any, error) {
		entries, err := k.store.List(listOptions.Prefix, listOptions.Limit)
		if err != nil {
			return nil, err
		}
		jsEntries := make([]ListEntry, len(entries))
		for i, entry := range entries {
			jsEntries[i] = ListEntry{Key: entry.Key, Value: entry.Value}
		}
		return k.vu.Runtime().ToValue(jsEntries), nil
	})
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
}

// ImportListOptions instantiates a ListOptions from a sobek.Value.
func ImportListOptions(rt *sobek.Runtime, options sobek.Value) ListOptions {
	listOptions := ListOptions{}

	if common.IsNullish(options) {
		return listOptions
	}

	optionsObj := options.ToObject(rt)
	listOptions.Prefix = optionsObj.Get("prefix").String()

	limitValue := optionsObj.Get("limit")
	if limitValue == nil {
		return listOptions
	}

	var limit int64
	if err := rt.ExportTo(limitValue, &limit); err == nil {
		listOptions.Limit = limit
	}

	return listOptions
}
