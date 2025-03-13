// Package kv provides a key-value database that can be used to store and retrieve data.
//
// The key-value database is backed by BoltDB, and is shared between all VUs. It is persisted
// to disk, so data stored in the database will be available across test runs.
//
// The database is opened when the first KV instance is created, and closed when the last KV
// instance is closed.
package kv

import (
	"fmt"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"

	"github.com/oleiade/xk6-kv/kv/store"
)

type (
	// RootModule is the global module instance that will create Client
	// instances for each VU.
	RootModule struct {
		// db *db
		store store.Store
	}

	// ModuleInstance represents an instance of the JS module.
	ModuleInstance struct {
		vu modules.VU
		rm *RootModule

		kv *KV
	}
)

// Ensure the interfaces are implemented correctly
var (
	_ modules.Instance = &ModuleInstance{}
	_ modules.Module   = &RootModule{}
)

// New returns a pointer to a new RootModule instance
func New() *RootModule {
	// // Create a memory store with JSON serialization by default
	// memoryStore := store.NewMemoryStore()
	// jsonSerializer := store.NewJSONSerializer()
	// serializedStore := store.NewSerializedStore(memoryStore, jsonSerializer)

	// return &RootModule{store: serializedStore}
	return &RootModule{
		// As default, the store is nil, we expect the user to call openKv()
		// which should set the store shared between all VUs.
		store: nil,
	}
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
	return modules.Exports{Named: map[string]interface{}{
		"openKv": mi.OpenKv,
	}}
}

// NewKV implements the modules.Instance interface and returns
// a new KV instance.
func (mi *ModuleInstance) NewKV(_ sobek.ConstructorCall) *sobek.Object {
	return mi.vu.Runtime().ToValue(mi.kv).ToObject(mi.vu.Runtime())
}

// OpenKv opens the KV store and returns a KV instance.
func (mi *ModuleInstance) OpenKv(opts sobek.Value) *sobek.Object {
	options, err := NewOptionsFrom(mi.vu, opts)
	if err != nil {
		common.Throw(mi.vu.Runtime(), err)
		return nil
	}

	if mi.rm.store == nil {
		// Create the base store based on the backend option
		var baseStore store.Store
		switch options.Backend {
		case "memory":
			baseStore = store.NewMemoryStore()
		case "disk":
			baseStore = store.NewDiskStore()
		}

		// Create the serializer based on the serialization option
		var serializer store.Serializer
		switch options.Serialization {
		case "json":
			serializer = store.NewJSONSerializer()
		case "string":
			serializer = store.NewStringSerializer()
		default:
			serializer = store.NewJSONSerializer() // Default to JSON
		}

		// Create a serialized store with the chosen store and serializer
		serializedStore := store.NewSerializedStore(baseStore, serializer)
		mi.rm.store = serializedStore
	}

	kv := NewKV(mi.vu, mi.rm.store)
	mi.kv = kv

	return mi.vu.Runtime().ToValue(mi.kv).ToObject(mi.vu.Runtime())
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
