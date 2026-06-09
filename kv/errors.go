package kv

// ErrorName identifies a class of error surfaced to JS as the `name` field
// of an Error instance. Each named error is mapped from a Go-side condition
// at the JS boundary in kv.go.
type ErrorName string

const (
	// keyNotFoundErr is emitted when a lookup is performed for a key that
	// does not exist in the store. The string value is the name surfaced to
	// JS, kept stable for script-side `err.name === "KeyNotFoundError"`
	// checks.
	keyNotFoundErr ErrorName = "KeyNotFoundError"
)

// Error represents a custom error emitted by the kv module to JS scripts.
// Only fields are exported (so encoding/json can see them); construction is
// package-private via newError.
type Error struct {
	// Name contains one of the strings associated with an error name.
	Name ErrorName `json:"name"`

	// Message represents message or description associated with the given error name.
	Message string `json:"message"`
}

func newError(name ErrorName, message string) *Error {
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
