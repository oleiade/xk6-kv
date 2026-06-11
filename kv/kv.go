package kv

import (
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

		result, err := k.store.AtomicCommit(nil, []store.Mutation{{
			Type:  store.MutationSet,
			Key:   keyString,
			Value: exportedValue,
		}})
		if err != nil {
			reject(err)
			return
		}

		resolve(map[string]any{
			"ok":           result.Ok,
			"versionstamp": result.Versionstamp,
		})
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

		entry, err := k.store.GetEntry(keyString)
		if err != nil {
			reject(err)
			return
		}

		resolve(entryFromStore(entry))
	}()

	return promise
}

// GetMany returns entries for the given keys in the same order as the input keys.
func (k *KV) GetMany(keys sobek.Value) *sobek.Promise {
	promise, resolve, reject := promises.New(k.vu)

	var keyStrings []string
	if err := k.vu.Runtime().ExportTo(keys, &keyStrings); err != nil {
		go func() {
			reject(err)
		}()
		return promise
	}

	go func() {
		if k.store == nil {
			reject(NewError(DatabaseNotOpenError, "database is not open"))
			return
		}

		entries, err := k.store.GetMany(keyStrings)
		if err != nil {
			reject(err)
			return
		}

		jsEntries := make([]Entry, len(entries))
		for i, entry := range entries {
			jsEntries[i] = entryFromStore(entry)
		}

		resolve(k.vu.Runtime().ToValue(jsEntries))
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

// Atomic creates an atomic operation builder.
func (k *KV) Atomic() *AtomicOperation {
	return &AtomicOperation{
		kv: k,
	}
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

		// Convert entries to Entry format for JavaScript
		jsEntries := make([]Entry, len(entries))
		for i, entry := range entries {
			jsEntries[i] = entryFromStore(entry)
		}

		resolve(k.vu.Runtime().ToValue(jsEntries))
	}()

	return promise
}

// AtomicOperation is a Deno KV-style optimistic atomic operation builder.
type AtomicOperation struct {
	kv        *KV
	checks    []store.Check
	mutations []store.Mutation
	committed bool
}

// Check adds versionstamp preconditions to the operation.
func (a *AtomicOperation) Check(checkValues ...sobek.Value) *AtomicOperation {
	for _, checkValue := range checkValues {
		a.checks = append(a.checks, importCheck(a.kv.vu.Runtime(), checkValue))
	}

	return a
}

// Set adds a set mutation to the operation.
func (a *AtomicOperation) Set(key sobek.Value, value sobek.Value) *AtomicOperation {
	a.mutations = append(a.mutations, store.Mutation{
		Type:  store.MutationSet,
		Key:   key.String(),
		Value: value.Export(),
	})

	return a
}

// Delete adds a delete mutation to the operation.
func (a *AtomicOperation) Delete(key sobek.Value) *AtomicOperation {
	a.mutations = append(a.mutations, store.Mutation{
		Type: store.MutationDelete,
		Key:  key.String(),
	})

	return a
}

// Commit commits all checks and mutations as a single atomic operation.
func (a *AtomicOperation) Commit() *sobek.Promise {
	promise, resolve, reject := promises.New(a.kv.vu)

	if a.committed {
		go func() {
			reject(NewError(AtomicOperationError, "atomic operation has already been committed"))
		}()
		return promise
	}
	a.committed = true
	checks := append([]store.Check(nil), a.checks...)
	mutations := append([]store.Mutation(nil), a.mutations...)

	go func() {
		if a.kv.store == nil {
			reject(NewError(DatabaseNotOpenError, "database is not open"))
			return
		}

		result, err := a.kv.store.AtomicCommit(checks, mutations)
		if err != nil {
			reject(err)
			return
		}
		if !result.Ok {
			resolve(map[string]any{"ok": false})
			return
		}

		resolve(map[string]any{
			"ok":           true,
			"versionstamp": result.Versionstamp,
		})
	}()

	return promise
}

// Entry is a versioned key-value pair returned by get, getMany, and list.
type Entry struct {
	Key          string `json:"key"`
	Value        any    `json:"value"`
	Versionstamp any    `json:"versionstamp"`
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

func entryFromStore(entry store.Entry) Entry {
	if entry.Versionstamp == "" {
		return Entry{
			Key:          entry.Key,
			Value:        nil,
			Versionstamp: nil,
		}
	}

	return Entry{
		Key:          entry.Key,
		Value:        entry.Value,
		Versionstamp: entry.Versionstamp,
	}
}

func importCheck(rt *sobek.Runtime, value sobek.Value) store.Check {
	obj := value.ToObject(rt)
	check := store.Check{
		Key: obj.Get("key").String(),
	}

	versionstampValue := obj.Get("versionstamp")
	if common.IsNullish(versionstampValue) {
		return check
	}

	check.Versionstamp = versionstampValue.String()
	return check
}
