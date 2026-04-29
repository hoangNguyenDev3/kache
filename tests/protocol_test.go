package test

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/hoangNguyenDev3/kache/resp"
	"github.com/stretchr/testify/assert"
)

func TestSimpleString(t *testing.T) {
	value := resp.NewSimpleString("OK")
	assert.Equal(t, byte('+'), value.Type)
	assert.Equal(t, "OK", value.Str)

	marshaled := value.Marshal()
	assert.Equal(t, "+OK\r\n", string(marshaled))

	parsed, err := resp.Parse(bufio.NewReader(bytes.NewReader(marshaled)))
	assert.NoError(t, err)
	assert.Equal(t, value.Type, parsed.Type)
	assert.Equal(t, value.Str, parsed.Str)
}

func TestError(t *testing.T) {
	testErr := errors.New("test error")
	value := resp.NewError(testErr)
	assert.Equal(t, byte('-'), value.Type)
	assert.Equal(t, testErr, value.Err)

	marshaled := value.Marshal()
	assert.Equal(t, "-test error\r\n", string(marshaled))

	parsed, err := resp.Parse(bufio.NewReader(bytes.NewReader(marshaled)))
	assert.NoError(t, err)
	assert.Equal(t, value.Type, parsed.Type)
	assert.Equal(t, value.Err.Error(), parsed.Err.Error())
}

func TestInteger(t *testing.T) {
	value := resp.NewInteger(42)
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(42), value.Int)

	marshaled := value.Marshal()
	assert.Equal(t, ":42\r\n", string(marshaled))

	parsed, err := resp.Parse(bufio.NewReader(bytes.NewReader(marshaled)))
	assert.NoError(t, err)
	assert.Equal(t, value.Type, parsed.Type)
	assert.Equal(t, value.Int, parsed.Int)
}

func TestBulkString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "normal string",
			input: "hello world",
			want:  "$11\r\nhello world\r\n",
		},
		{
			name:  "empty string",
			input: "",
			want:  "$0\r\n\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := resp.NewBulkString(tt.input)
			assert.Equal(t, byte('$'), value.Type)
			assert.Equal(t, tt.input, value.Str)

			marshaled := value.Marshal()
			assert.Equal(t, tt.want, string(marshaled))

			parsed, err := resp.Parse(bufio.NewReader(bytes.NewReader(marshaled)))
			assert.NoError(t, err)
			assert.Equal(t, value.Type, parsed.Type)
			assert.Equal(t, value.Str, parsed.Str)
		})
	}
}

func TestArray(t *testing.T) {
	array := []resp.Value{
		resp.NewBulkString("SET"),
		resp.NewBulkString("key"),
		resp.NewBulkString("value"),
	}
	value := resp.NewArray(array)
	assert.Equal(t, byte('*'), value.Type)
	assert.Equal(t, array, value.Array)

	marshaled := value.Marshal()
	expected := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	assert.Equal(t, expected, string(marshaled))

	parsed, err := resp.Parse(bufio.NewReader(bytes.NewReader(marshaled)))
	assert.NoError(t, err)
	assert.Equal(t, value.Type, parsed.Type)
	assert.Equal(t, len(value.Array), len(parsed.Array))
	for i := range value.Array {
		assert.Equal(t, value.Array[i].Type, parsed.Array[i].Type)
		assert.Equal(t, value.Array[i].Str, parsed.Array[i].Str)
	}
}

func TestInvalidInput(t *testing.T) {
	tests := []struct {
		want  error
		name  string
		input string
	}{
		{
			name:  "invalid type",
			input: "?invalid\r\n",
			want:  errors.New("unknown type byte: ?"),
		},
		{
			name:  "missing CRLF",
			input: "+OK",
			want:  io.EOF,
		},
		{
			name:  "invalid bulk string length",
			input: "$-2\r\n",
			want:  errors.New("invalid bulk string length: -2"),
		},
		{
			name:  "invalid array length",
			input: "*-2\r\n",
			want:  errors.New("invalid array length: -2"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resp.Parse(bufio.NewReader(bytes.NewReader([]byte(tt.input))))
			assert.Error(t, err)
			if tt.want != nil {
				assert.Equal(t, tt.want.Error(), err.Error())
			}
		})
	}
}

func BenchmarkMarshal(b *testing.B) {
	array := []resp.Value{
		resp.NewBulkString("SET"),
		resp.NewBulkString("key"),
		resp.NewBulkString("value"),
	}
	value := resp.NewArray(array)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value.Marshal()
	}
}

func BenchmarkParse(b *testing.B) {
	array := []resp.Value{
		resp.NewBulkString("SET"),
		resp.NewBulkString("key"),
		resp.NewBulkString("value"),
	}
	value := resp.NewArray(array)
	data := value.Marshal()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp.Parse(bufio.NewReader(bytes.NewReader(data)))
	}
}
