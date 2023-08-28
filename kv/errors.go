package kv

// ErrorName represents the name of an error
type ErrorName string

const (
	// DatabaseNotOpenError is emitted when the database is accessed before it is opened
	// or after it is closed.
	DatabaseNotOpenError ErrorName = "DatabaseNotOpenError"

	// DatabaseAlreadyOpenError is emitted when the database is opened more than once.
	DatabaseAlreadyOpenError = "DatabaseAlreadyOpenError"

	// BucketNotFoundError is emitted when the bucket is not found in the database.
	BucketNotFoundError = "BucketNotFoundError"

	// BucketExistsError is emitted when the bucket already exists in the database.
	BucketExistsError = "BucketExistsError"

	// KeyNotFoundError is emitted when the key is not found in the bucket.
	KeyNotFoundError = "KeyNotFoundError"

	// KeyRequiredError is emitted when inserting an empty key.
	KeyRequiredError = "KeyRequiredError"

	// KeyTooLargeError is emitted when the key is too large.
	KeyTooLargeError = "KeyTooLargeError"

	// ValueTooLargeError is emitted when the value is too large.
	ValueTooLargeError = "ValueTooLargeError"
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
