package test

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/hoangNguyenDev3/kache/resp"
	"github.com/stretchr/testify/assert"
)

func sendCommands(t *testing.T, addr string, cmds []string) []resp.Value {
	conn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer conn.Close()

	reader := bufio.NewReader(conn)
	results := make([]resp.Value, len(cmds))

	for i, cmd := range cmds {
		_, err = conn.Write([]byte(cmd))
		assert.NoError(t, err)

		value, err := resp.Parse(reader)
		assert.NoError(t, err)
		results[i] = value
	}

	return results
}

func TestMultiExec(t *testing.T) {
	_, addr := setupTCPServer(t)

	results := sendCommands(t, addr, []string{
		"*1\r\n$5\r\nMULTI\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
		"*1\r\n$4\r\nEXEC\r\n",
	})

	// MULTI
	assert.Equal(t, byte('+'), results[0].Type)
	assert.Equal(t, "OK", results[0].Str)

	// SET (queued)
	assert.Equal(t, byte('+'), results[1].Type)
	assert.Equal(t, "QUEUED", results[1].Str)

	// GET (queued)
	assert.Equal(t, byte('+'), results[2].Type)
	assert.Equal(t, "QUEUED", results[2].Str)

	// EXEC
	assert.Equal(t, byte('*'), results[3].Type)
	assert.Equal(t, 2, len(results[3].Array))

	// SET result
	assert.Equal(t, byte('+'), results[3].Array[0].Type)
	assert.Equal(t, "OK", results[3].Array[0].Str)

	// GET result
	assert.Equal(t, byte('$'), results[3].Array[1].Type)
	assert.Equal(t, "value", results[3].Array[1].Str)
}

func TestDiscard(t *testing.T) {
	_, addr := setupTCPServer(t)

	results := sendCommands(t, addr, []string{
		"*1\r\n$5\r\nMULTI\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
		"*1\r\n$7\r\nDISCARD\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
	})

	// MULTI
	assert.Equal(t, byte('+'), results[0].Type)
	assert.Equal(t, "OK", results[0].Str)

	// SET (queued)
	assert.Equal(t, byte('+'), results[1].Type)
	assert.Equal(t, "QUEUED", results[1].Str)

	// DISCARD
	assert.Equal(t, byte('+'), results[2].Type)
	assert.Equal(t, "OK", results[2].Str)

	// GET should return empty since transaction was discarded
	assert.Equal(t, byte('$'), results[3].Type)
	assert.Equal(t, "", results[3].Str)
}

func TestExecWithoutMulti(t *testing.T) {
	_, addr := setupTCPServer(t)

	result := sendCommand(t, addr, "*1\r\n$4\r\nEXEC\r\n")
	assert.Equal(t, byte('-'), result.Type)
	assert.Contains(t, result.Err.Error(), "EXEC without MULTI")
}

func TestNestedMulti(t *testing.T) {
	_, addr := setupTCPServer(t)

	results := sendCommands(t, addr, []string{
		"*1\r\n$5\r\nMULTI\r\n",
		"*1\r\n$5\r\nMULTI\r\n",
	})

	// First MULTI
	assert.Equal(t, byte('+'), results[0].Type)
	assert.Equal(t, "OK", results[0].Str)

	// Nested MULTI
	assert.Equal(t, byte('-'), results[1].Type)
	assert.Contains(t, results[1].Err.Error(), "MULTI calls can not be nested")
}

func TestDiscardWithoutMulti(t *testing.T) {
	_, addr := setupTCPServer(t)

	result := sendCommand(t, addr, "*1\r\n$7\r\nDISCARD\r\n")
	assert.Equal(t, byte('-'), result.Type)
	assert.Contains(t, result.Err.Error(), "DISCARD without MULTI")
}

func TestConcurrentTransactions(t *testing.T) {
	_, addr := setupTCPServer(t)
	const numClients = 10

	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func() {
			defer wg.Done()
			results := sendCommands(t, addr, []string{
				"*1\r\n$5\r\nMULTI\r\n",
				"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n",
				"*1\r\n$4\r\nEXEC\r\n",
			})

			// MULTI
			assert.Equal(t, byte('+'), results[0].Type)
			assert.Equal(t, "OK", results[0].Str)

			// INCR queued
			assert.Equal(t, byte('+'), results[1].Type)
			assert.Equal(t, "QUEUED", results[1].Str)

			// EXEC
			assert.Equal(t, byte('*'), results[2].Type)
			assert.Equal(t, 1, len(results[2].Array))

			// INCR result should be an integer
			assert.Equal(t, byte(':'), results[2].Array[0].Type)
		}()
	}

	wg.Wait()

	// Verify final counter value
	value := sendCommand(t, addr, "*2\r\n$3\r\nGET\r\n$7\r\ncounter\r\n")
	assert.Equal(t, byte('$'), value.Type)
	assert.Equal(t, fmt.Sprintf("%d", numClients), value.Str)
}
