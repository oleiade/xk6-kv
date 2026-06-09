// Package store provides a key-value store interface and implementations.
//
// The package exposes two interfaces:
//
//   - Backend is the low-level, []byte-typed contract that storage
//     implementations (in-memory, BoltDB, ...) satisfy. It carries no
//     serialization concern; values are uninterpreted byte slices.
//
//   - Store is the JS-facing contract that delivers deserialized values
//     (any). SerializedStore is its canonical implementation, decorating a
//     Backend with a Serializer.
//
// The split lets the rest of the codebase enforce at compile time that
// backends only ever produce []byte, while the JS layer continues to see
// JSON-decoded values.
package store

// Backend is the raw []byte-typed key-value contract that storage
// implementations satisfy.
type Backend interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	Exists(key string) (bool, error)
	Clear() error
	Size() (int64, error)
	List(prefix string, limit int64) ([]RawEntry, error)
	Close() error
}

// RawEntry is a key/value pair returned by Backend.List, with the value as
// raw bytes.
type RawEntry struct {
	Key   string
	Value []byte
}

// Store is the JS-facing key-value contract: values cross the boundary as
// arbitrary deserialized Go values.
type Store interface {
	Get(key string) (any, error)
	Set(key string, value any) error
	Delete(key string) error
	Exists(key string) (bool, error)
	Clear() error
	Size() (int64, error)
	List(prefix string, limit int64) ([]Entry, error)
	Close() error
}

// Entry is a key/value pair returned by Store.List, with the value already
// deserialized.
type Entry struct {
	Key   string
	Value any
}
