package test

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/hoangNguyenDev3/kache/pubsub"
	"github.com/hoangNguyenDev3/kache/resp"
	"github.com/hoangNguyenDev3/kache/server"
	"github.com/hoangNguyenDev3/kache/store"
	"github.com/stretchr/testify/assert"
)

func setupPubSubServer(t *testing.T) (*server.TCPServer, string) {
	s := store.New(nil)
	tcpConfig := &server.Config{
		ClientTimeout: 30 * time.Second,
	}
	ps := pubsub.New()
	tcpServer := server.NewTCPServer(s, tcpConfig, ps)

	listener, err := net.Listen("tcp", ":0")
	assert.NoError(t, err)
	addr := listener.Addr().String()
	listener.Close()

	go func() {
		if err := tcpServer.Start(addr); err != nil {
			t.Errorf("Failed to start TCP server: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return tcpServer, addr
}

func TestPubSubSubscribeAndPublish(t *testing.T) {
	_, addr := setupPubSubServer(t)

	// Connect subscriber
	subConn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer subConn.Close()

	subReader := bufio.NewReader(subConn)

	// Send SUBSCRIBE
	_, err = subConn.Write(resp.FormatCommand([]string{"SUBSCRIBE", "testchannel"}))
	assert.NoError(t, err)

	// Read subscribe confirmation
	value, err := resp.Parse(subReader)
	assert.NoError(t, err)
	assert.Equal(t, byte('*'), value.Type)
	assert.Equal(t, 3, len(value.Array))
	assert.Equal(t, "subscribe", value.Array[0].Str)
	assert.Equal(t, "testchannel", value.Array[1].Str)
	assert.Equal(t, int64(1), value.Array[2].Int)

	// Connect publisher
	pubConn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer pubConn.Close()

	pubReader := bufio.NewReader(pubConn)

	// Publish a message
	_, err = pubConn.Write(resp.FormatCommand([]string{"PUBLISH", "testchannel", "hello"}))
	assert.NoError(t, err)

	// Read publish response (should be 1 subscriber)
	value, err = resp.Parse(pubReader)
	assert.NoError(t, err)
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(1), value.Int)

	// Read message on subscriber connection
	value, err = resp.Parse(subReader)
	assert.NoError(t, err)
	assert.Equal(t, byte('*'), value.Type)
	assert.Equal(t, 3, len(value.Array))
	assert.Equal(t, "message", value.Array[0].Str)
	assert.Equal(t, "testchannel", value.Array[1].Str)
	assert.Equal(t, "hello", value.Array[2].Str)

	// Send UNSUBSCRIBE
	_, err = subConn.Write(resp.FormatCommand([]string{"UNSUBSCRIBE", "testchannel"}))
	assert.NoError(t, err)

	// Read unsubscribe confirmation
	value, err = resp.Parse(subReader)
	assert.NoError(t, err)
	assert.Equal(t, byte('*'), value.Type)
	assert.Equal(t, 3, len(value.Array))
	assert.Equal(t, "unsubscribe", value.Array[0].Str)
	assert.Equal(t, "testchannel", value.Array[1].Str)
	assert.Equal(t, int64(0), value.Array[2].Int)
}

func TestPubSubMultipleSubscribers(t *testing.T) {
	_, addr := setupPubSubServer(t)

	// Connect two subscribers
	subConn1, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer subConn1.Close()
	subReader1 := bufio.NewReader(subConn1)

	subConn2, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer subConn2.Close()
	subReader2 := bufio.NewReader(subConn2)

	// Subscribe both to same channel
	_, err = subConn1.Write(resp.FormatCommand([]string{"SUBSCRIBE", "news"}))
	assert.NoError(t, err)
	_, err = subConn2.Write(resp.FormatCommand([]string{"SUBSCRIBE", "news"}))
	assert.NoError(t, err)

	// Read confirmations
	value, err := resp.Parse(subReader1)
	assert.NoError(t, err)
	assert.Equal(t, "subscribe", value.Array[0].Str)

	value, err = resp.Parse(subReader2)
	assert.NoError(t, err)
	assert.Equal(t, "subscribe", value.Array[0].Str)

	// Connect publisher and publish
	pubConn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer pubConn.Close()
	pubReader := bufio.NewReader(pubConn)

	_, err = pubConn.Write(resp.FormatCommand([]string{"PUBLISH", "news", "breaking"}))
	assert.NoError(t, err)

	// Should receive 2 subscribers
	value, err = resp.Parse(pubReader)
	assert.NoError(t, err)
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(2), value.Int)

	// Both subscribers should receive the message
	value, err = resp.Parse(subReader1)
	assert.NoError(t, err)
	assert.Equal(t, "message", value.Array[0].Str)
	assert.Equal(t, "news", value.Array[1].Str)
	assert.Equal(t, "breaking", value.Array[2].Str)

	value, err = resp.Parse(subReader2)
	assert.NoError(t, err)
	assert.Equal(t, "message", value.Array[0].Str)
	assert.Equal(t, "news", value.Array[1].Str)
	assert.Equal(t, "breaking", value.Array[2].Str)

	// Unsubscribe both
	_, err = subConn1.Write(resp.FormatCommand([]string{"UNSUBSCRIBE"}))
	assert.NoError(t, err)
	_, err = subConn2.Write(resp.FormatCommand([]string{"UNSUBSCRIBE"}))
	assert.NoError(t, err)
}

func TestPubSubPublishNoSubscribers(t *testing.T) {
	_, addr := setupPubSubServer(t)

	pubConn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer pubConn.Close()
	pubReader := bufio.NewReader(pubConn)

	_, err = pubConn.Write(resp.FormatCommand([]string{"PUBLISH", "emptychannel", "msg"}))
	assert.NoError(t, err)

	value, err := resp.Parse(pubReader)
	assert.NoError(t, err)
	assert.Equal(t, byte(':'), value.Type)
	assert.Equal(t, int64(0), value.Int)
}

func TestPubSubMultipleChannels(t *testing.T) {
	_, addr := setupPubSubServer(t)

	subConn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer subConn.Close()
	subReader := bufio.NewReader(subConn)

	// Subscribe to multiple channels at once
	_, err = subConn.Write(resp.FormatCommand([]string{"SUBSCRIBE", "ch1", "ch2"}))
	assert.NoError(t, err)

	// Read two confirmations
	value, err := resp.Parse(subReader)
	assert.NoError(t, err)
	assert.Equal(t, "subscribe", value.Array[0].Str)
	assert.Equal(t, "ch1", value.Array[1].Str)
	assert.Equal(t, int64(1), value.Array[2].Int)

	value, err = resp.Parse(subReader)
	assert.NoError(t, err)
	assert.Equal(t, "subscribe", value.Array[0].Str)
	assert.Equal(t, "ch2", value.Array[1].Str)
	assert.Equal(t, int64(2), value.Array[2].Int)

	// Publish to ch1
	pubConn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer pubConn.Close()
	pubReader := bufio.NewReader(pubConn)

	_, err = pubConn.Write(resp.FormatCommand([]string{"PUBLISH", "ch1", "msg1"}))
	assert.NoError(t, err)

	value, err = resp.Parse(pubReader)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), value.Int)

	// Should receive on ch1
	value, err = resp.Parse(subReader)
	assert.NoError(t, err)
	assert.Equal(t, "message", value.Array[0].Str)
	assert.Equal(t, "ch1", value.Array[1].Str)
	assert.Equal(t, "msg1", value.Array[2].Str)

	// Unsubscribe from ch1 only
	_, err = subConn.Write(resp.FormatCommand([]string{"UNSUBSCRIBE", "ch1"}))
	assert.NoError(t, err)

	value, err = resp.Parse(subReader)
	assert.NoError(t, err)
	assert.Equal(t, "unsubscribe", value.Array[0].Str)
	assert.Equal(t, "ch1", value.Array[1].Str)
	assert.Equal(t, int64(1), value.Array[2].Int)

	// Publish to ch1 again - should receive 0
	pubConn2, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer pubConn2.Close()
	pubReader2 := bufio.NewReader(pubConn2)

	_, err = pubConn2.Write(resp.FormatCommand([]string{"PUBLISH", "ch1", "msg2"}))
	assert.NoError(t, err)

	value, err = resp.Parse(pubReader2)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), value.Int)

	// Unsubscribe from all
	_, err = subConn.Write(resp.FormatCommand([]string{"UNSUBSCRIBE"}))
	assert.NoError(t, err)

	value, err = resp.Parse(subReader)
	if err != nil {
		t.Fatalf("Failed to parse unsubscribe response: %v", err)
	}
	assert.Equal(t, "unsubscribe", value.Array[0].Str)
	assert.Equal(t, "ch2", value.Array[1].Str)
	assert.Equal(t, int64(0), value.Array[2].Int)
}
