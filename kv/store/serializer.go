package store

import (
	"encoding/json"
	"fmt"
)

// Serializer defines the interface for serializing and deserializing values.
type Serializer interface {
	// Serialize converts a value to a byte slice.
	Serialize(value any) ([]byte, error)

	// Deserialize converts a byte slice back to a value.
	Deserialize(data []byte) (any, error)
}

// JSONSerializer implements the Serializer interface using JSON encoding.
type JSONSerializer struct{}

// Ensure JSONSerializer implements the Serializer interface.
var _ Serializer = &JSONSerializer{}

// NewJSONSerializer creates a new JSONSerializer.
func NewJSONSerializer() *JSONSerializer {
	return &JSONSerializer{}
}

// Serialize converts a value to a JSON byte slice.
func (s *JSONSerializer) Serialize(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize value to JSON: %w", err)
	}
	return data, nil
}

// Deserialize converts a JSON byte slice back to a value.
func (s *JSONSerializer) Deserialize(data []byte) (any, error) {
	var err error

	if len(data) == 0 {
		return nil, err
	}

	var value any
	if err = json.Unmarshal(data, &value); err != nil {
		return nil, fmt.Errorf("unable to deserialize JSON value: %w", err)
	}
	return value, nil
}

// StringSerializer is a simple serializer that treats values as strings.
// This is useful for simple string-based storage.
type StringSerializer struct{}

// Ensure StringSerializer implements the Serializer interface.
var _ Serializer = &StringSerializer{}

// NewStringSerializer creates a new StringSerializer.
func NewStringSerializer() *StringSerializer {
	return &StringSerializer{}
}

// Serialize converts a value to a string and then to bytes.
func (s *StringSerializer) Serialize(value any) ([]byte, error) {
	// For string values, just convert to bytes
	if str, ok := value.(string); ok {
		return []byte(str), nil
	}

	// For other values, try to convert to string
	return []byte(fmt.Sprintf("%v", value)), nil
}

// Deserialize converts bytes back to a string.
func (s *StringSerializer) Deserialize(data []byte) (any, error) {
	return string(data), nil
}
