package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

const (
	MaxArrayLength      = 1048576           // 1M elements max per array
	MaxBulkStringLength = 512 * 1024 * 1024 // 512MB max per bulk string
)

var (
	ErrInvalidSyntax = errors.New("invalid RESP syntax")
	CRLF             = []byte{'\r', '\n'}
)

// Value represents a RESP value
type Value struct {
	Type  byte
	Str   string
	Int   int64
	Array []Value
	Err   error
}

// NewSimpleString creates a new RESP simple string
func NewSimpleString(s string) Value {
	return Value{Type: SimpleString, Str: s}
}

// NewError creates a new RESP error
func NewError(err error) Value {
	return Value{Type: Error, Err: err}
}

// NewInteger creates a new RESP integer
func NewInteger(i int64) Value {
	return Value{Type: Integer, Int: i}
}

// NewBulkString creates a new RESP bulk string
func NewBulkString(s string) Value {
	return Value{Type: BulkString, Str: s}
}

// NewArray creates a new RESP array
func NewArray(a []Value) Value {
	return Value{Type: Array, Array: a}
}

// Marshal serializes a Value into RESP format
func (v Value) Marshal() []byte {
	buf := bytes.Buffer{}

	switch v.Type {
	case SimpleString:
		buf.WriteByte(SimpleString)
		buf.WriteString(v.Str)
	case Error:
		buf.WriteByte(Error)
		if v.Err != nil {
			buf.WriteString(v.Err.Error())
		}
	case Integer:
		buf.WriteByte(Integer)
		buf.WriteString(strconv.FormatInt(v.Int, 10))
	case BulkString:
		buf.WriteByte(BulkString)
		buf.WriteString(strconv.Itoa(len(v.Str)))
		buf.Write(CRLF)
		buf.WriteString(v.Str)
	case Array:
		buf.WriteByte(Array)
		buf.WriteString(strconv.Itoa(len(v.Array)))
		buf.Write(CRLF)
		for _, item := range v.Array {
			buf.Write(item.Marshal())
		}
		return buf.Bytes()
	}

	buf.Write(CRLF)
	return buf.Bytes()
}

// Parse reads a RESP value from a reader
func Parse(r *bufio.Reader) (Value, error) {
	// Read the type byte
	typ, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch typ {
	case SimpleString:
		return parseSimpleString(r)
	case Error:
		return parseError(r)
	case Integer:
		return parseInteger(r)
	case BulkString:
		return parseBulkString(r)
	case Array:
		return parseArray(r)
	default:
		return Value{}, fmt.Errorf("unknown type byte: %c", typ)
	}
}

func parseSimpleString(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	return NewSimpleString(string(line)), nil
}

func parseError(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	return NewError(errors.New(string(line))), nil
}

func parseInteger(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return Value{}, err
	}
	return NewInteger(n), nil
}

func parseBulkString(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}

	length, err := strconv.Atoi(string(line))
	if err != nil {
		return Value{}, fmt.Errorf("invalid bulk string length: %w", err)
	}

	// Handle null bulk string
	if length == -1 {
		return Value{Type: BulkString}, nil
	}

	// Validate length
	if length < 0 {
		return Value{}, fmt.Errorf("invalid bulk string length: %d", length)
	}
	if length > MaxBulkStringLength {
		return Value{}, fmt.Errorf("bulk string length %d exceeds maximum allowed %d", length, MaxBulkStringLength)
	}

	// Read the string data
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return Value{}, fmt.Errorf("failed to read bulk string data: %w", err)
	}

	// Read the trailing CRLF
	if _, err := readLine(r); err != nil {
		return Value{}, fmt.Errorf("failed to read bulk string terminator: %w", err)
	}

	return NewBulkString(string(data)), nil
}

func parseArray(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}

	length, err := strconv.Atoi(string(line))
	if err != nil {
		return Value{}, fmt.Errorf("invalid array length: %w", err)
	}

	// Handle null array
	if length == -1 {
		return Value{Type: Array}, nil
	}

	// Validate length
	if length < 0 {
		return Value{}, fmt.Errorf("invalid array length: %d", length)
	}
	if length > MaxArrayLength {
		return Value{}, fmt.Errorf("array length %d exceeds maximum allowed %d", length, MaxArrayLength)
	}

	array := make([]Value, length)
	for i := 0; i < length; i++ {
		value, err := Parse(r)
		if err != nil {
			return Value{}, fmt.Errorf("failed to parse array element %d: %w", i, err)
		}
		array[i] = value
	}

	return NewArray(array), nil
}

// readLine reads until CRLF and returns the line without CRLF
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, ErrInvalidSyntax
	}

	return line[:len(line)-2], nil
}

// Write writes a RESP value to the writer
func Write(w io.Writer, value *Value) error {
	_, err := w.Write(value.Marshal())
	return err
}

// WriteError writes a RESP error to the writer
func WriteError(w io.Writer, message string) error {
	_, err := w.Write(NewError(fmt.Errorf(message)).Marshal())
	return err
}

// Reader reads RESP commands from a connection
type Reader struct {
	reader *bufio.Reader
}

// NewReader creates a new RESP reader
func NewReader(conn net.Conn) *Reader {
	return &Reader{
		reader: bufio.NewReader(conn),
	}
}

// ReadCommand reads a command from the reader
func (r *Reader) ReadCommand() ([]string, error) {
	value, err := Parse(r.reader)
	if err != nil {
		return nil, err
	}

	if value.Type != Array {
		return nil, fmt.Errorf("expected array, got %c", value.Type)
	}

	result := make([]string, len(value.Array))
	for i, v := range value.Array {
		if v.Type != BulkString && v.Type != SimpleString {
			return nil, fmt.Errorf("expected bulk string, got %c", v.Type)
		}
		result[i] = v.Str
	}

	return result, nil
}

// FormatCommand formats a command as a RESP array
func FormatCommand(cmd []string) []byte {
	array := make([]Value, len(cmd))
	for i, s := range cmd {
		array[i] = NewBulkString(s)
	}
	return NewArray(array).Marshal()
}
