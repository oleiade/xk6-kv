// Package kv provides a key-value database that can be used to store and retrieve data.
//
// The key-value database is backed by BoltDB, and is shared between all VUs. It is persisted
// to disk, so data stored in the database will be available across test runs.
//
// The database is opened when the first KV instance is created, and closed when the last KV
// instance is closed.
package kv

import (
	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

type (
	// RootModule is the global module instance that will create Client
	// instances for each VU.
	RootModule struct {
		db *db
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
	return &RootModule{db: newDB()}
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
func (mi *ModuleInstance) OpenKv() *sobek.Object {
	if err := mi.rm.db.open(); err != nil {
		common.Throw(mi.vu.Runtime(), err)
		return nil
	}

	kv := NewKV(mi.vu, mi.rm.db)
	kv.bucket = []byte(DefaultKvBucket)
	mi.kv = kv

	return mi.vu.Runtime().ToValue(mi.kv).ToObject(mi.vu.Runtime())
}

const (
	// DefaultKvPath is the default path to the KV store
	DefaultKvPath = ".k6.kv"

	// DefaultKvBucket is the default bucket name for the KV store
	DefaultKvBucket = "k6"
)
