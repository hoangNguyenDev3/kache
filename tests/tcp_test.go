package test

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/hoangNguyenDev3/redis-clone/resp"
	"github.com/hoangNguyenDev3/redis-clone/server"
	"github.com/hoangNguyenDev3/redis-clone/store"
	"github.com/stretchr/testify/assert"
)

func setupTCPServer(t *testing.T) (*server.TCPServer, string) {
	s := store.New(nil)
	tcpConfig := &server.Config{
		ClientTimeout: 30 * time.Second,
	}
	tcpServer := server.NewTCPServer(s, tcpConfig)

	// Start server on random port
	listener, err := net.Listen("tcp", ":0")
	assert.NoError(t, err)
	addr := listener.Addr().String()
	listener.Close()

	go func() {
		if err := tcpServer.Start(addr); err != nil {
			t.Errorf("Failed to start TCP server: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)
	return tcpServer, addr
}

func sendCommand(t *testing.T, addr string, cmd string) resp.Value {
	conn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer conn.Close()

	// Write command
	_, err = conn.Write([]byte(cmd))
	assert.NoError(t, err)

	// Read response
	reader := bufio.NewReader(conn)
	value, err := resp.Parse(reader)
	assert.NoError(t, err)
	return value
}

func TestPing(t *testing.T) {
	_, addr := setupTCPServer(t)

	// Test PING
	value := sendCommand(t, addr, "*1\r\n$4\r\nPING\r\n")
	assert.Equal(t, byte('+'), value.Type)
	assert.Equal(t, "PONG", value.Str)
}

func TestSetGet(t *testing.T) {
	_, addr := setupTCPServer(t)

	// Test SET
	value := sendCommand(t, addr, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	assert.Equal(t, byte('+'), value.Type)
	assert.Equal(t, "OK", value.Str)

	// Test GET
	value = sendCommand(t, addr, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
	assert.Equal(t, byte('$'), value.Type)
	assert.Equal(t, "value", value.Str)
}

func TestDel(t *testing.T) {
	_, addr := setupTCPServer(t)

	// Set up test data
	sendCommand(t, addr, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	sendCommand(t, addr, "*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n")

	// Test DEL single key
	value := sendCommand(t, addr, "*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n")
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(1), value.Int)

	// Test DEL multiple keys
	value = sendCommand(t, addr, "*3\r\n$3\r\nDEL\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n")
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(1), value.Int)
}

func TestIncr(t *testing.T) {
	_, addr := setupTCPServer(t)

	// Test INCR new key
	value := sendCommand(t, addr, "*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n")
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(1), value.Int)

	// Test INCR existing key
	value = sendCommand(t, addr, "*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n")
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(2), value.Int)
}

func TestHashCommands(t *testing.T) {
	_, addr := setupTCPServer(t)

	// Test HSET
	value := sendCommand(t, addr, "*4\r\n$4\r\nHSET\r\n$4\r\nuser\r\n$4\r\nname\r\n$4\r\nJohn\r\n")
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(1), value.Int)

	// Test HGET
	value = sendCommand(t, addr, "*3\r\n$4\r\nHGET\r\n$4\r\nuser\r\n$4\r\nname\r\n")
	assert.Equal(t, byte('$'), value.Type)
	assert.Equal(t, "John", value.Str)

	// Test HDEL
	value = sendCommand(t, addr, "*3\r\n$4\r\nHDEL\r\n$4\r\nuser\r\n$4\r\nname\r\n")
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(1), value.Int)

	// Test HGETALL
	value = sendCommand(t, addr, "*2\r\n$7\r\nHGETALL\r\n$4\r\nuser\r\n")
	assert.Equal(t, byte('*'), value.Type)
	assert.Equal(t, 0, len(value.Array))

	// Test HLEN
	value = sendCommand(t, addr, "*2\r\n$4\r\nHLEN\r\n$4\r\nuser\r\n")
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(0), value.Int)
}

func TestExpireCommands(t *testing.T) {
	_, addr := setupTCPServer(t)

	// Set up test data
	sendCommand(t, addr, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")

	// Test EXPIRE
	value := sendCommand(t, addr, "*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n60\r\n")
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(1), value.Int)

	// Test TTL
	value = sendCommand(t, addr, "*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n")
	assert.Equal(t, byte(':'), value.Type)
	assert.True(t, value.Int > 0 && value.Int <= 60)
}

func TestKeys(t *testing.T) {
	_, addr := setupTCPServer(t)

	// Set up test data
	sendCommand(t, addr, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	sendCommand(t, addr, "*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n")

	// Test KEYS
	value := sendCommand(t, addr, "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n")
	assert.Equal(t, byte('*'), value.Type)
	assert.Equal(t, 2, len(value.Array))

	// Convert array to string slice for easier comparison
	keys := make([]string, len(value.Array))
	for i, v := range value.Array {
		keys[i] = v.Str
	}
	assert.Contains(t, keys, "key")
	assert.Contains(t, keys, "key2")
}

func TestErrorHandling(t *testing.T) {
	_, addr := setupTCPServer(t)

	// Test invalid command
	value := sendCommand(t, addr, "*1\r\n$7\r\nINVALID\r\n")
	assert.Equal(t, byte('-'), value.Type)
	assert.Contains(t, value.Err.Error(), "unknown command")

	// Test wrong number of arguments
	value = sendCommand(t, addr, "*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n")
	assert.Equal(t, byte('-'), value.Type)
	assert.Contains(t, value.Err.Error(), "wrong number of arguments")

	// Test invalid key
	value = sendCommand(t, addr, "*2\r\n$3\r\nGET\r\n$0\r\n\r\n")
	assert.Equal(t, byte('-'), value.Type)
	assert.Contains(t, value.Err.Error(), "invalid key")
}
