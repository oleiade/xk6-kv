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

	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/common"
	"go.k6.io/k6/v2/js/modules"

	"github.com/oleiade/xk6-kv/kv/store"
)

type (
	// RootModule is the global module instance that will create Client
	// instances for each VU.
	RootModule struct {
		store store.Store
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
func (mi *ModuleInstance) OpenKv(opts sobek.Value) *sobek.Object {
	options, err := NewOptionsFrom(mi.vu, opts)
	if err != nil {
		common.Throw(mi.vu.Runtime(), err)
		return nil
	}

	if mi.rm.store == nil {
		var backend store.Backend
		switch options.Backend {
		case "memory":
			backend = store.NewMemoryStore()
		case "disk":
			backend = store.NewDiskStore(store.DefaultDiskStorePath)
		}

		var serializer store.Serializer
		switch options.Serialization {
		case "json":
			serializer = store.NewJSONSerializer()
		case "string":
			serializer = store.NewStringSerializer()
		default:
			serializer = store.NewJSONSerializer()
		}

		mi.rm.store = store.NewSerializedStore(backend, serializer)
	}

	kv := NewKV(mi.vu, mi.rm.store)
	return mi.vu.Runtime().ToValue(kv).ToObject(mi.vu.Runtime())
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

// NewOptionsFrom creates a new KVOptions instance from a sobek.Value.
func NewOptionsFrom(vu modules.VU, options sobek.Value) (Options, error) {
	// Default
	opts := Options{
		Backend:       DefaultBackend,
		Serialization: DefaultSerialization,
	}

	if common.IsNullish(options) {
		return opts, nil
	}

	if err := vu.Runtime().ExportTo(options, &opts); err != nil {
		return opts, fmt.Errorf("unable to parse options; reason: %w", err)
	}

	if opts.Backend != "memory" && opts.Backend != "disk" {
		return opts, fmt.Errorf("invalid backend: %s, valid values are: %s, %s", opts.Backend, DefaultBackend, "disk")
	}

	if opts.Serialization != "json" && opts.Serialization != "string" {
		return opts, fmt.Errorf(
			"invalid serialization: %s, valid values are: %s, %s",
			opts.Serialization, DefaultSerialization, "string",
		)
	}

	return opts, nil
}

const (
	// DefaultBackend is the default backend to use for the KV store
	DefaultBackend = "disk"

	// DefaultSerialization is the default serialization format
	DefaultSerialization = "json"
)
