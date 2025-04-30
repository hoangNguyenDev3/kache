package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConcurrentIncrements(t *testing.T) {
	_, addr := setupTCPServer(t)
	const numClients = 10
	const incrementsPerClient = 100

	var wg sync.WaitGroup
	wg.Add(numClients)

	// Run multiple clients incrementing the same key
	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			for j := 0; j < incrementsPerClient; j++ {
				value := sendCommand(t, addr, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n")
				assert.Equal(t, byte(':'), value.Type)
			}
		}(i)
	}

	wg.Wait()

	// Verify final counter value
	value := sendCommand(t, addr, "*2\r\n$3\r\nGET\r\n$7\r\ncounter\r\n")
	assert.Equal(t, byte('$'), value.Type)
	assert.Equal(t, fmt.Sprintf("%d", numClients*incrementsPerClient), value.Str)
}

func TestConcurrentSetsGets(t *testing.T) {
	_, addr := setupTCPServer(t)
	const numClients = 10
	const opsPerClient = 100

	var wg sync.WaitGroup
	wg.Add(numClients)

	// Run multiple clients setting and getting different keys
	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("key%d_%d", clientID, j)
				value := fmt.Sprintf("value%d_%d", clientID, j)

				// SET key
				resp := sendCommand(t, addr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
					len(key), key, len(value), value))
				assert.Equal(t, byte('+'), resp.Type)
				assert.Equal(t, "OK", resp.Str)

				// GET key
				resp = sendCommand(t, addr, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n",
					len(key), key))
				assert.Equal(t, byte('$'), resp.Type)
				assert.Equal(t, value, resp.Str)
			}
		}(i)
	}

	wg.Wait()
}

func TestConcurrentHashOperations(t *testing.T) {
	_, addr := setupTCPServer(t)
	const numClients = 10
	const opsPerClient = 100

	var wg sync.WaitGroup
	wg.Add(numClients)

	// Run multiple clients performing hash operations
	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			hashKey := fmt.Sprintf("hash%d", clientID)

			for j := 0; j < opsPerClient; j++ {
				field := fmt.Sprintf("field%d", j)
				value := fmt.Sprintf("value%d_%d", clientID, j)

				// HSET
				resp := sendCommand(t, addr, fmt.Sprintf("*4\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
					len(hashKey), hashKey, len(field), field, len(value), value))
				assert.Equal(t, byte(':'), resp.Type)

				// HGET
				resp = sendCommand(t, addr, fmt.Sprintf("*3\r\n$4\r\nHGET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
					len(hashKey), hashKey, len(field), field))
				assert.Equal(t, byte('$'), resp.Type)
				assert.Equal(t, value, resp.Str)
			}

			// HLEN
			resp := sendCommand(t, addr, fmt.Sprintf("*2\r\n$4\r\nHLEN\r\n$%d\r\n%s\r\n",
				len(hashKey), hashKey))
			assert.Equal(t, byte(':'), resp.Type)
			assert.Equal(t, int64(opsPerClient), resp.Int)
		}(i)
	}

	wg.Wait()
}

func TestConcurrentExpiry(t *testing.T) {
	_, addr := setupTCPServer(t)
	const numClients = 10
	const opsPerClient = 10

	var wg sync.WaitGroup
	wg.Add(numClients)

	// Run multiple clients setting expiring keys
	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("expiry%d_%d", clientID, j)
				value := fmt.Sprintf("value%d_%d", clientID, j)

				// SET key
				resp := sendCommand(t, addr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
					len(key), key, len(value), value))
				assert.Equal(t, byte('+'), resp.Type)
				assert.Equal(t, "OK", resp.Str)

				// EXPIRE key
				resp = sendCommand(t, addr, fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$1\r\n1\r\n",
					len(key), key))
				assert.Equal(t, byte(':'), resp.Type)
				assert.Equal(t, int64(1), resp.Int)
			}
		}(i)
	}

	wg.Wait()

	// Wait for keys to expire
	time.Sleep(2 * time.Second)

	// Verify all keys have expired
	for i := 0; i < numClients; i++ {
		for j := 0; j < opsPerClient; j++ {
			key := fmt.Sprintf("expiry%d_%d", i, j)
			resp := sendCommand(t, addr, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n",
				len(key), key))
			assert.Equal(t, byte('$'), resp.Type)
			assert.Equal(t, "", resp.Str) // Key should be expired
		}
	}
}
