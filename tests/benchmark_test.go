package test

import (
	"bufio"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoangNguyenDev3/kache/pubsub"
	"github.com/hoangNguyenDev3/kache/resp"
	"github.com/hoangNguyenDev3/kache/server"
	"github.com/hoangNguyenDev3/kache/store"
)

// --- Direct store benchmarks (no network overhead) ---

func BenchmarkStoreSet(b *testing.B) {
	s := store.New(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		s.Set(key, "value", nil)
	}
}

func BenchmarkStoreGet(b *testing.B) {
	s := store.New(nil)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		s.Set(key, "value", nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%10000)
		s.Get(key)
	}
}

func BenchmarkStoreIncr(b *testing.B) {
	s := store.New(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Incr("counter")
	}
}

// --- Parallel benchmarks to demonstrate multi-core scaling ---

func BenchmarkParallelSet(b *testing.B) {
	s := store.New(nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for pb.Next() {
			key := fmt.Sprintf("key-%d", atomic.AddInt64(&i, 1))
			s.Set(key, "value", nil)
		}
	})
}

func BenchmarkParallelGet(b *testing.B) {
	s := store.New(nil)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		s.Set(key, "value", nil)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for pb.Next() {
			key := fmt.Sprintf("key-%d", atomic.AddInt64(&i, 1)%10000)
			s.Get(key)
		}
	})
}

func BenchmarkParallelMixed(b *testing.B) {
	s := store.New(nil)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		s.Set(key, "value", nil)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			if n%5 == 0 {
				// 20% writes
				key := fmt.Sprintf("key-%d", n%10000)
				s.Set(key, "value", nil)
			} else {
				// 80% reads
				key := fmt.Sprintf("key-%d", n%10000)
				s.Get(key)
			}
		}
	})
}

// --- TCP benchmark helpers ---

func setupTCPServerForBenchmark(b *testing.B) (*server.TCPServer, string) {
	s := store.New(nil)
	tcpConfig := &server.Config{
		ClientTimeout: 30 * time.Second,
	}
	ps := pubsub.New()
	tcpServer := server.NewTCPServer(s, tcpConfig, ps)

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	go func() {
		if err := tcpServer.Start(addr); err != nil {
			b.Errorf("failed to start TCP server: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return tcpServer, addr
}

func sendTCPCommand(b *testing.B, conn net.Conn, cmd []byte) resp.Value {
	_, err := conn.Write(cmd)
	if err != nil {
		b.Fatalf("failed to write command: %v", err)
	}
	reader := bufio.NewReader(conn)
	value, err := resp.Parse(reader)
	if err != nil {
		b.Fatalf("failed to parse response: %v", err)
	}
	return value
}

// --- TCP benchmarks (measures full protocol stack) ---

func BenchmarkTCPSet(b *testing.B) {
	tcpServer, addr := setupTCPServerForBenchmark(b)
	defer tcpServer.Stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	cmd := resp.FormatCommand([]string{"SET", "benchkey", "benchvalue"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Write(cmd)
		if err != nil {
			b.Fatalf("failed to write: %v", err)
		}
		reader := bufio.NewReader(conn)
		_, err = resp.Parse(reader)
		if err != nil {
			b.Fatalf("failed to parse response: %v", err)
		}
	}
}

func BenchmarkTCPGet(b *testing.B) {
	tcpServer, addr := setupTCPServerForBenchmark(b)
	defer tcpServer.Stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		cmd := resp.FormatCommand([]string{"SET", key, "value"})
		sendTCPCommand(b, conn, cmd)
	}

	getCmd := resp.FormatCommand([]string{"GET", "key-0"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Write(getCmd)
		if err != nil {
			b.Fatalf("failed to write: %v", err)
		}
		reader := bufio.NewReader(conn)
		_, err = resp.Parse(reader)
		if err != nil {
			b.Fatalf("failed to parse response: %v", err)
		}
	}
}

func BenchmarkTCPPipeline(b *testing.B) {
	tcpServer, addr := setupTCPServerForBenchmark(b)
	defer tcpServer.Stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Build a batch of 100 SET commands
	batchSize := 100
	var batch []byte
	for i := 0; i < batchSize; i++ {
		key := fmt.Sprintf("pipekey-%d", i)
		batch = append(batch, resp.FormatCommand([]string{"SET", key, "value"})...)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Write(batch)
		if err != nil {
			b.Fatalf("failed to write batch: %v", err)
		}
		reader := bufio.NewReader(conn)
		for j := 0; j < batchSize; j++ {
			_, err := resp.Parse(reader)
			if err != nil {
				b.Fatalf("failed to parse response %d: %v", j, err)
			}
		}
	}
	b.SetBytes(int64(len(batch)))
}
