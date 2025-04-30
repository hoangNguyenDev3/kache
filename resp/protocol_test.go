package resp

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestValue_Marshal(t *testing.T) {
	testCases := []struct {
		name     string
		value    Value
		expected []byte
	}{
		{
			name:     "simple string",
			value:    NewSimpleString("OK"),
			expected: []byte("+OK\r\n"),
		},
		{
			name:     "error",
			value:    NewError(errors.New("Error message")),
			expected: []byte("-Error message\r\n"),
		},
		{
			name:     "integer",
			value:    NewInteger(42),
			expected: []byte(":42\r\n"),
		},
		{
			name:     "bulk string",
			value:    NewBulkString("hello"),
			expected: []byte("$5\r\nhello\r\n"),
		},
		{
			name: "array",
			value: NewArray([]Value{
				NewBulkString("SET"),
				NewBulkString("key"),
				NewBulkString("value"),
			}),
			expected: []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.value.Marshal()
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestParse(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected Value
		wantErr  bool
	}{
		{
			name:     "simple string",
			input:    "+OK\r\n",
			expected: NewSimpleString("OK"),
			wantErr:  false,
		},
		{
			name:     "error",
			input:    "-Error message\r\n",
			expected: NewError(errors.New("Error message")),
			wantErr:  false,
		},
		{
			name:     "integer",
			input:    ":42\r\n",
			expected: NewInteger(42),
			wantErr:  false,
		},
		{
			name:     "bulk string",
			input:    "$5\r\nhello\r\n",
			expected: NewBulkString("hello"),
			wantErr:  false,
		},
		{
			name:  "array",
			input: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
			expected: NewArray([]Value{
				NewBulkString("SET"),
				NewBulkString("key"),
				NewBulkString("value"),
			}),
			wantErr: false,
		},
		{
			name:     "invalid type",
			input:    "X42\r\n",
			expected: Value{},
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tc.input))
			result, err := Parse(reader)

			if (err != nil) != tc.wantErr {
				t.Errorf("Expected err: %v, got: %v", tc.wantErr, err)
				return
			}

			if tc.wantErr {
				return
			}

			if result.Type != tc.expected.Type {
				t.Errorf("Expected type %c, got %c", tc.expected.Type, result.Type)
			}

			switch result.Type {
			case SimpleString, BulkString:
				if result.Str != tc.expected.Str {
					t.Errorf("Expected %s, got %s", tc.expected.Str, result.Str)
				}
			case Integer:
				if result.Int != tc.expected.Int {
					t.Errorf("Expected %d, got %d", tc.expected.Int, result.Int)
				}
			case Error:
				if result.Err.Error() != tc.expected.Err.Error() {
					t.Errorf("Expected %s, got %s", tc.expected.Err.Error(), result.Err.Error())
				}
			case Array:
				if len(result.Array) != len(tc.expected.Array) {
					t.Errorf("Expected array of length %d, got %d", len(tc.expected.Array), len(result.Array))
				}
				for i := range result.Array {
					if result.Array[i].Type != tc.expected.Array[i].Type {
						t.Errorf("Array element %d: expected type %c, got %c", i, tc.expected.Array[i].Type, result.Array[i].Type)
					}
					if result.Array[i].Str != tc.expected.Array[i].Str {
						t.Errorf("Array element %d: expected %s, got %s", i, tc.expected.Array[i].Str, result.Array[i].Str)
					}
				}
			}
		})
	}
}

// MockConn is a mock net.Conn for testing
type MockConn struct {
	buffer bytes.Buffer
}

func (m *MockConn) Read(b []byte) (n int, err error) {
	return m.buffer.Read(b)
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	return m.buffer.Write(b)
}

func (m *MockConn) Close() error                       { return nil }
func (m *MockConn) LocalAddr() net.Addr                { return nil }
func (m *MockConn) RemoteAddr() net.Addr               { return nil }
func (m *MockConn) SetDeadline(t time.Time) error      { return nil }
func (m *MockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *MockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestWrite(t *testing.T) {
	conn := &MockConn{}
	value := NewSimpleString("OK")
	err := Write(conn, &value)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	expected := []byte("+OK\r\n")
	if !reflect.DeepEqual(conn.buffer.Bytes(), expected) {
		t.Errorf("Expected %v, got %v", expected, conn.buffer.Bytes())
	}
}

func TestWriteError(t *testing.T) {
	conn := &MockConn{}
	err := WriteError(conn, "Error message")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	expected := []byte("-Error message\r\n")
	if !reflect.DeepEqual(conn.buffer.Bytes(), expected) {
		t.Errorf("Expected %v, got %v", expected, conn.buffer.Bytes())
	}
}

func TestFormatCommand(t *testing.T) {
	cmd := []string{"SET", "key", "value"}
	result := FormatCommand(cmd)
	expected := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestReadCommand(t *testing.T) {
	// Create a mock connection with a RESP command
	conn := &MockConn{}
	conn.buffer.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))

	// Create a reader and read the command
	reader := NewReader(conn)
	cmd, err := reader.ReadCommand()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Check the result
	expected := []string{"SET", "key", "value"}
	if !reflect.DeepEqual(cmd, expected) {
		t.Errorf("Expected %v, got %v", expected, cmd)
	}
}
