package store

import "errors"

// ErrKeyNotFound is returned (wrapped) by Backend implementations when a key
// lookup misses. Callers can detect it with errors.Is and translate it into
// whatever shape their layer prefers; in the kv module, the JS boundary maps
// it to a structured "KeyNotFoundError" visible from script code.
var ErrKeyNotFound = errors.New("key not found")
