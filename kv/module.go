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
	"sort"
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
		// storeOnce serializes the lazy initialization of the shared store. The
		// first openKv() call to win the race constructs the backend; later
		// callers either reuse it or, if their options conflict, get an error.
		storeOnce sync.Once
		store     store.Store
		storeErr  error
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
// read from this map so the two paths cannot drift apart.
//
//nolint:gochecknoglobals
var backendFactories = map[string]func() store.Backend{
	"memory": func() store.Backend { return store.NewMemoryStore() },
	"disk":   func() store.Backend { return store.NewDiskStore(store.DefaultDiskStorePath) },
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

	mi.rm.storeOnce.Do(func() {
		mi.rm.store, mi.rm.storeErr = buildStore(options)
		if mi.rm.storeErr == nil {
			mi.rm.storeOpts = options
		}
	})
	if mi.rm.storeErr != nil {
		common.Throw(mi.vu.Runtime(), mi.rm.storeErr)
		return nil
	}

	if mi.rm.storeOpts != options {
		common.Throw(mi.vu.Runtime(), fmt.Errorf(
			"kv module already initialized with backend=%q serialization=%q; "+
				"cannot reopen with backend=%q serialization=%q",
			mi.rm.storeOpts.Backend, mi.rm.storeOpts.Serialization,
			options.Backend, options.Serialization,
		))
		return nil
	}

	kv := NewKV(mi.vu, mi.rm.store)
	return mi.vu.Runtime().ToValue(kv).ToObject(mi.vu.Runtime())
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
	return store.NewSerializedStore(backendFactory(), serializerFactory()), nil
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
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

const (
	// DefaultBackend is the default backend to use for the KV store
	DefaultBackend = "disk"

	// DefaultSerialization is the default serialization format
	DefaultSerialization = "json"
)
