// Package kv provides a key-value store JS module that can be used to share state across
// k6 Virtual Users during a test run.
//
// The store is shared between all VUs and constructed once on the first call to openKv().
// Two backends are available: an in-memory backend (ephemeral, fastest) and a disk backend
// (BoltDB-backed, persistent across test runs). The backend and serialization format are
// selected via the options passed to openKv().
package kv

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/common"
	"go.k6.io/k6/v2/js/modules"

	"github.com/oleiade/xk6-kv/kv/store"
)

type (
	// RootModule is the global module instance that creates KV instances for each VU.
	RootModule struct {
		// mu serializes the lazy initialization of the shared store and
		// subsequent reads of store/storeOpts. The first openKv() call
		// to win the lock constructs the backend; later callers either
		// reuse it or, if their options conflict, get an error. Init
		// failures are NOT latched — a transient bolt.Open error on the
		// first call doesn't permanently break the module; the next call
		// gets a fresh attempt.
		mu        sync.Mutex
		store     store.Store
		storeOpts Options
	}

	// ModuleInstance represents an instance of the JS module.
	ModuleInstance struct {
		vu modules.VU
		rm *RootModule
	}
)

// Ensure the interfaces are implemented correctly
var (
	_ modules.Instance = &ModuleInstance{}
	_ modules.Module   = &RootModule{}
)

// backendFactories registers the available raw storage backends. Adding a new
// backend is a single map entry; OpenKv's validation and construction both
// read from this map so the two paths cannot drift apart. The uniform
// `(Backend, error)` signature lets backends that may fail at construction
// (e.g. disk, which acquires a BoltDB file lock) share the call site with
// infallible ones (e.g. memory).
//
//nolint:gochecknoglobals
var backendFactories = map[string]func() (store.Backend, error){
	"memory": newMemoryBackend,
	"disk":   newDiskBackend,
}

// newMemoryBackend always returns a nil error; the signature matches the
// fallible backends so they can all share the factory map.
//
//nolint:unparam
func newMemoryBackend() (store.Backend, error) { return store.NewMemoryStore(), nil }

func newDiskBackend() (store.Backend, error) {
	return store.NewDiskStore(store.DefaultDiskStorePath)
}

// serializerFactories registers the available serializers. See backendFactories.
//
//nolint:gochecknoglobals
var serializerFactories = map[string]func() store.Serializer{
	"json":   func() store.Serializer { return store.NewJSONSerializer() },
	"string": func() store.Serializer { return store.NewStringSerializer() },
}

// New returns a pointer to a new RootModule instance.
//
// The shared store is left nil and is constructed on the first call to openKv()
// from JS, so its backend and serialization can be chosen at script time.
func New() *RootModule {
	return &RootModule{}
}

// NewModuleInstance implements the modules.Module interface and returns
// a new instance for each VU.
func (rm *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ModuleInstance{
		vu: vu,
		rm: rm,
	}
}

// Exports implements the modules.Instance interface and returns
// the exports of the JS module.
func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{Named: map[string]any{
		"openKv": mi.OpenKv,
	}}
}

// OpenKv opens the KV store and returns a KV instance.
//
// The shared store is constructed once per process on the first call. Later
// calls reuse that store. If a later call requests options that conflict with
// the options the store was first opened with, it errors out — VUs cannot
// safely "reopen" the store with a different backend or serializer.
func (mi *ModuleInstance) OpenKv(opts sobek.Value) *sobek.Object {
	options, err := NewOptionsFrom(mi.vu, opts)
	if err != nil {
		common.Throw(mi.vu.Runtime(), err)
		return nil
	}

	sharedStore, storeOpts, err := mi.rm.acquireStore(options)
	if err != nil {
		common.Throw(mi.vu.Runtime(), err)
		return nil
	}
	if storeOpts != options {
		common.Throw(mi.vu.Runtime(), fmt.Errorf(
			"kv module already initialized with backend=%q serialization=%q; "+
				"cannot reopen with backend=%q serialization=%q",
			storeOpts.Backend, storeOpts.Serialization,
			options.Backend, options.Serialization,
		))
		return nil
	}

	kv := NewKV(mi.vu, sharedStore)
	return mi.vu.Runtime().ToValue(kv).ToObject(mi.vu.Runtime())
}

// acquireStore returns the shared store, constructing it on the first call.
// Concurrent callers serialize on rm.mu; the second-and-later callers see
// the store the winner built. On construction failure the slot is left
// empty so a subsequent call can retry — a transient bolt.Open error
// doesn't permanently break the module.
func (rm *RootModule) acquireStore(options Options) (store.Store, Options, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.store == nil {
		s, err := buildStore(options)
		if err != nil {
			return nil, Options{}, err
		}
		rm.store = s
		rm.storeOpts = options
	}
	return rm.store, rm.storeOpts, nil
}

// buildStore constructs a Store from validated options by composing a backend
// and a serializer via the SerializedStore decorator.
func buildStore(options Options) (store.Store, error) {
	backendFactory, ok := backendFactories[options.Backend]
	if !ok {
		return nil, fmt.Errorf("unknown backend: %q", options.Backend)
	}
	serializerFactory, ok := serializerFactories[options.Serialization]
	if !ok {
		return nil, fmt.Errorf("unknown serialization: %q", options.Serialization)
	}
	backend, err := backendFactory()
	if err != nil {
		return nil, fmt.Errorf("build %s backend: %w", options.Backend, err)
	}
	return store.NewSerializedStore(backend, serializerFactory()), nil
}

// Options represents the options for a KV instance.
type Options struct {
	// Backend is the backend to use for the KV instance.
	//
	// Valid values are "memory" and "disk".
	Backend string `json:"backend"`

	// Serialization is the serialization format to use.
	//
	// Valid values are "json" and "string".
	Serialization string `json:"serialization"`
}

// NewOptionsFrom creates a new Options instance from a sobek.Value, applying
// defaults and validating the backend/serialization against the registered
// factories.
func NewOptionsFrom(vu modules.VU, options sobek.Value) (Options, error) {
	opts := Options{
		Backend:       DefaultBackend,
		Serialization: DefaultSerialization,
	}

	if common.IsNullish(options) {
		return opts, nil
	}

	if err := vu.Runtime().ExportTo(options, &opts); err != nil {
		return Options{}, fmt.Errorf("unable to parse options; reason: %w", err)
	}

	if _, ok := backendFactories[opts.Backend]; !ok {
		return Options{}, fmt.Errorf(
			"invalid backend: %q (valid: %s)", opts.Backend, sortedKeys(backendFactories))
	}
	if _, ok := serializerFactories[opts.Serialization]; !ok {
		return Options{}, fmt.Errorf(
			"invalid serialization: %q (valid: %s)", opts.Serialization, sortedKeys(serializerFactories))
	}

	return opts, nil
}

// sortedKeys returns the comma-separated sorted keys of a map for stable
// error messages.
func sortedKeys[V any](m map[string]V) string {
	return strings.Join(slices.Sorted(maps.Keys(m)), ", ")
}

const (
	// DefaultBackend is the default backend to use for the KV store
	DefaultBackend = "disk"

	// DefaultSerialization is the default serialization format
	DefaultSerialization = "json"
)
