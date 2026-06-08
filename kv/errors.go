package kv

// ErrorName identifies a class of error surfaced to JS as the `name` field
// of an Error instance. Each named error is mapped from a Go-side condition
// at the JS boundary in kv.go.
type ErrorName string

const (
	// DatabaseNotOpenError is emitted when the key-value store is accessed
	// before openKv() has been called.
	DatabaseNotOpenError ErrorName = "DatabaseNotOpenError"

	// KeyNotFoundError is emitted when a lookup is performed for a key that
	// does not exist in the store.
	KeyNotFoundError ErrorName = "KeyNotFoundError"
)

// Error represents a custom error emitted by the kv module
type Error struct {
	// Name contains one of the strings associated with an error name.
	Name ErrorName `json:"name"`

	// Message represents message or description associated with the given error name.
	Message string `json:"message"`
}

// NewError returns a new Error instance.
func NewError(name ErrorName, message string) *Error {
	return &Error{
		Name:    name,
		Message: message,
	}
}

// Error implements the `error` interface
func (e *Error) Error() string {
	return string(e.Name) + ": " + e.Message
}

var _ error = (*Error)(nil)
