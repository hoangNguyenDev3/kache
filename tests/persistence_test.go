package test

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoangNguyenDev3/redis-clone/server"
	"github.com/hoangNguyenDev3/redis-clone/store"
	"github.com/stretchr/testify/assert"
)

func TestRDBPersistence(t *testing.T) {
	// Create temporary directory for RDB file
	tmpDir, err := os.MkdirTemp("", "redis-clone-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	rdbPath := filepath.Join(tmpDir, "dump.rdb")

	// Create store with RDB persistence
	s := store.New(&store.StoreConfig{
		RDBPath: rdbPath,
	})

	// Create and start server
	tcpConfig := &server.Config{
		ClientTimeout: 30 * time.Second,
	}
	tcpServer := server.NewTCPServer(s, tcpConfig)

	// Start server on random port
	listener, err := net.Listen("tcp", ":0")
	assert.NoError(t, err)
	addr := listener.Addr().String()
	listener.Close()

	// Start server in a goroutine
	serverErr := make(chan error)
	go func() {
		serverErr <- tcpServer.Start(addr)
	}()
	time.Sleep(100 * time.Millisecond)

	// Set some test data
	sendCommand(t, addr, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	sendCommand(t, addr, "*4\r\n$4\r\nHSET\r\n$4\r\nuser\r\n$4\r\nname\r\n$4\r\nJohn\r\n")

	// Trigger RDB save and wait for it to complete
	err = s.SaveRDB(rdbPath)
	assert.NoError(t, err)

	// Stop server and wait for it to finish
	err = tcpServer.Stop()
	assert.NoError(t, err)

	// Create new store and server with same RDB file
	s2 := store.New(&store.StoreConfig{
		RDBPath: rdbPath,
	})

	// Load RDB file
	err = s2.LoadRDB(rdbPath)
	assert.NoError(t, err)

	tcpServer2 := server.NewTCPServer(s2, tcpConfig)

	// Start new server on random port
	listener2, err := net.Listen("tcp", ":0")
	assert.NoError(t, err)
	addr2 := listener2.Addr().String()
	listener2.Close()

	// Start second server in a goroutine
	go func() {
		serverErr <- tcpServer2.Start(addr2)
	}()
	time.Sleep(100 * time.Millisecond)

	// Verify data was restored
	value := sendCommand(t, addr2, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
	assert.Equal(t, byte('$'), value.Type)
	assert.Equal(t, "value", value.Str)

	value = sendCommand(t, addr2, "*3\r\n$4\r\nHGET\r\n$4\r\nuser\r\n$4\r\nname\r\n")
	assert.Equal(t, byte('$'), value.Type)
	assert.Equal(t, "John", value.Str)

	// Stop second server and wait for it to finish
	err = tcpServer2.Stop()
	assert.NoError(t, err)

	// Check for server errors
	select {
	case err := <-serverErr:
		assert.NoError(t, err)
	default:
	}
}

func TestReplication(t *testing.T) {
	t.Skip("Skipping replication test for now")
}

func TestReplicationReconnect(t *testing.T) {
	t.Skip("Skipping replication reconnect test for now")
}
