package resp

import (
	"bufio"
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		err      error
	}{
		{
			name:     "valid simple string",
			input:    "+OK\r\n",
			expected: NewSimpleString("OK"),
			err:      nil,
		},
		{
			name:     "empty simple string",
			input:    "+\r\n",
			expected: NewSimpleString(""),
			err:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewBufferString(tt.input))
			value, err := Parse(reader)
			if tt.err != nil {
				assert.Equal(t, tt.err, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestError(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		err      error
	}{
		{
			name:     "valid error",
			input:    "-Error message\r\n",
			expected: NewError(errors.New("Error message")),
			err:      nil,
		},
		{
			name:     "empty error",
			input:    "-\r\n",
			expected: NewError(errors.New("")),
			err:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewBufferString(tt.input))
			value, err := Parse(reader)
			if tt.err != nil {
				assert.Equal(t, tt.err, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected.Type, value.Type)
				assert.Equal(t, tt.expected.Err.Error(), value.Err.Error())
			}
		})
	}
}

func TestInteger(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		err      error
	}{
		{
			name:     "positive integer",
			input:    ":1234\r\n",
			expected: NewInteger(1234),
			err:      nil,
		},
		{
			name:     "negative integer",
			input:    ":-123\r\n",
			expected: NewInteger(-123),
			err:      nil,
		},
		{
			name:     "zero",
			input:    ":0\r\n",
			expected: NewInteger(0),
			err:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewBufferString(tt.input))
			value, err := Parse(reader)
			if tt.err != nil {
				assert.Equal(t, tt.err, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestBulkString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		err      error
	}{
		{
			name:     "valid bulk string",
			input:    "$5\r\nhello\r\n",
			expected: NewBulkString("hello"),
			err:      nil,
		},
		{
			name:     "empty bulk string",
			input:    "$0\r\n\r\n",
			expected: NewBulkString(""),
			err:      nil,
		},
		{
			name:     "null bulk string",
			input:    "$-1\r\n",
			expected: Value{Type: BulkString},
			err:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewBufferString(tt.input))
			value, err := Parse(reader)
			if tt.err != nil {
				assert.Equal(t, tt.err, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestArray(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		err      error
	}{
		{
			name:  "simple array",
			input: "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
			expected: NewArray([]Value{
				NewBulkString("hello"),
				NewBulkString("world"),
			}),
			err: nil,
		},
		{
			name:     "empty array",
			input:    "*0\r\n",
			expected: NewArray([]Value{}),
			err:      nil,
		},
		{
			name:     "null array",
			input:    "*-1\r\n",
			expected: Value{Type: Array},
			err:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewBufferString(tt.input))
			value, err := Parse(reader)
			if tt.err != nil {
				assert.Equal(t, tt.err, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestMarshal(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected string
	}{
		{
			name:     "simple string",
			value:    NewSimpleString("OK"),
			expected: "+OK\r\n",
		},
		{
			name:     "error",
			value:    NewError(errors.New("Error message")),
			expected: "-Error message\r\n",
		},
		{
			name:     "integer",
			value:    NewInteger(1234),
			expected: ":1234\r\n",
		},
		{
			name:     "bulk string",
			value:    NewBulkString("hello"),
			expected: "$5\r\nhello\r\n",
		},
		{
			name: "array",
			value: NewArray([]Value{
				NewBulkString("hello"),
				NewBulkString("world"),
			}),
			expected: "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := string(tt.value.Marshal())
			assert.Equal(t, tt.expected, result)
		})
	}
}
