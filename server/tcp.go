package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hoangNguyenDev3/redis-clone/resp"
	"github.com/hoangNguyenDev3/redis-clone/store"
)

// Client represents a connected client
type Client struct {
	conn     net.Conn
	server   *TCPServer
	lastSeen time.Time
}

// TCPServer represents a TCP server that handles RESP protocol
type TCPServer struct {
	listener net.Listener
	store    *store.Store
	clients  sync.Map
	config   *Config
	logger   *log.Logger
	done     chan struct{}
}

// Config holds TCP server configuration
type Config struct {
	ClientTimeout time.Duration
}

// NewTCPServer creates a new TCP server
func NewTCPServer(store *store.Store, config *Config) *TCPServer {
	return &TCPServer{
		store:  store,
		config: config,
		logger: log.New(log.Writer(), "[TCP] ", log.LstdFlags),
		done:   make(chan struct{}),
	}
}

// Start starts the TCP server
func (s *TCPServer) Start(addr string) error {
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start TCP server: %w", err)
	}

	s.logger.Printf("TCP server listening on %s", addr)

	go s.acceptConnections()
	return nil
}

// Stop stops the TCP server
func (s *TCPServer) Stop() error {
	// Signal shutdown
	close(s.done)

	// Close all client connections
	s.clients.Range(func(key, value interface{}) bool {
		if client, ok := key.(*Client); ok {
			client.conn.Close()
		}
		return true
	})

	// Close the listener
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
		s.listener = nil
	}
	return nil
}

func (s *TCPServer) acceptConnections() {
	for {
		select {
		case <-s.done:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				// Only log if not shutting down
				select {
				case <-s.done:
					return
				default:
					s.logger.Printf("Failed to accept connection: %v", err)
				}
				return
			}
			go s.handleConnection(conn)
		}
	}
}

func (s *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	client := &Client{
		conn:     conn,
		server:   s,
		lastSeen: time.Now(),
	}

	s.clients.Store(client, struct{}{})
	defer s.clients.Delete(client)

	reader := bufio.NewReader(conn)
	for {
		select {
		case <-s.done:
			return
		default:
			// Set read deadline
			if err := conn.SetReadDeadline(time.Now().Add(s.config.ClientTimeout)); err != nil {
				return
			}

			// Parse RESP command
			value, err := resp.Parse(reader)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					return
				}
				if err == io.EOF {
					return
				}
				// Send error response to client
				if err := resp.WriteError(conn, fmt.Sprintf("ERR %v", err)); err != nil {
					return
				}
				continue
			}

			// Process command
			response, err := s.processCommand(value)
			if err != nil {
				if err := resp.WriteError(conn, fmt.Sprintf("ERR %v", err)); err != nil {
					return
				}
				continue
			}

			// Send response
			if err := resp.Write(conn, &response); err != nil {
				return
			}

			// Update last seen time
			client.lastSeen = time.Now()
		}
	}
}

func (s *TCPServer) processCommand(value resp.Value) (resp.Value, error) {
	if value.Type != resp.Array {
		return resp.NewError(fmt.Errorf("ERR Protocol error: expected array, got %c", value.Type)), nil
	}

	if len(value.Array) == 0 {
		return resp.NewError(fmt.Errorf("ERR Protocol error: empty command")), nil
	}

	// Get command name
	cmdName := strings.ToUpper(value.Array[0].Str)
	if cmdName == "" {
		return resp.NewError(fmt.Errorf("ERR Protocol error: invalid command name")), nil
	}

	switch cmdName {
	case "PING":
		return resp.NewSimpleString("PONG"), nil

	case "SET":
		if len(value.Array) < 3 || len(value.Array) > 5 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'set' command")), nil
		}

		key := value.Array[1].Str
		if key == "" {
			return resp.NewError(fmt.Errorf("ERR Protocol error: invalid key")), nil
		}

		val := value.Array[2].Str

		var expiry *time.Time
		if len(value.Array) >= 4 && strings.ToUpper(value.Array[3].Str) == "EX" {
			if len(value.Array) != 5 {
				return resp.NewError(fmt.Errorf("ERR syntax error")), nil
			}
			seconds, err := strconv.Atoi(value.Array[4].Str)
			if err != nil {
				return resp.NewError(fmt.Errorf("ERR value is not an integer or out of range")), nil
			}
			t := time.Now().Add(time.Duration(seconds) * time.Second)
			expiry = &t
		}

		if err := s.store.Set(key, val, expiry); err != nil {
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}
		return resp.NewSimpleString("OK"), nil

	case "GET":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'get' command")), nil
		}

		key := value.Array[1].Str
		if key == "" {
			return resp.NewError(fmt.Errorf("ERR Protocol error: invalid key")), nil
		}

		val, err := s.store.Get(key)
		if err != nil {
			if err == store.ErrKeyNotFound || err == store.ErrKeyExpired {
				return resp.NewBulkString(""), nil
			}
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}

		// Convert value to string based on type
		var strVal string
		switch v := val.(type) {
		case string:
			strVal = v
		case int64:
			strVal = fmt.Sprintf("%d", v)
		case float64:
			strVal = fmt.Sprintf("%f", v)
		case map[string]string:
			return resp.NewError(store.ErrWrongType), nil
		default:
			return resp.NewError(store.ErrWrongType), nil
		}

		return resp.NewBulkString(strVal), nil

	case "INCR":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'incr' command")), nil
		}

		key := value.Array[1].Str
		if key == "" {
			return resp.NewError(fmt.Errorf("ERR Protocol error: invalid key")), nil
		}

		// Call store Incr which handles atomicity
		val, err := s.store.Incr(key)
		if err != nil {
			if err == store.ErrWrongType {
				return resp.NewError(fmt.Errorf("ERR value is not an integer or out of range")), nil
			}
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}

		// Return the new value as an integer
		return resp.NewInteger(val), nil

	case "DEL":
		if len(value.Array) < 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'del' command")), nil
		}

		keys := make([]string, len(value.Array)-1)
		for i := 1; i < len(value.Array); i++ {
			keys[i-1] = value.Array[i].Str
		}

		count, err := s.store.Del(keys...)
		if err != nil {
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}
		return resp.NewInteger(int64(count)), nil

	case "HSET":
		if len(value.Array) != 4 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'hset' command")), nil
		}

		key := value.Array[1].Str
		field := value.Array[2].Str
		val := value.Array[3].Str

		created, err := s.store.HSet(key, field, val)
		if err != nil {
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}
		if created {
			return resp.NewInteger(1), nil
		}
		return resp.NewInteger(0), nil

	case "HGET":
		if len(value.Array) != 3 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'hget' command")), nil
		}

		key := value.Array[1].Str
		field := value.Array[2].Str

		val, err := s.store.HGet(key, field)
		if err != nil {
			if err == store.ErrKeyNotFound || err == store.ErrKeyExpired {
				return resp.NewBulkString(""), nil
			}
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}
		return resp.NewBulkString(val), nil

	case "HDEL":
		if len(value.Array) < 3 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'hdel' command")), nil
		}

		key := value.Array[1].Str
		fields := make([]string, len(value.Array)-2)
		for i := 2; i < len(value.Array); i++ {
			fields[i-2] = value.Array[i].Str
		}

		count, err := s.store.HDel(key, fields...)
		if err != nil {
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}
		return resp.NewInteger(int64(count)), nil

	case "HGETALL":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'hgetall' command")), nil
		}

		key := value.Array[1].Str
		fields, err := s.store.HGetAll(key)
		if err != nil {
			if err == store.ErrKeyNotFound || err == store.ErrKeyExpired {
				return resp.NewArray([]resp.Value{}), nil
			}
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}

		// Convert map to array of field-value pairs
		result := make([]resp.Value, 0, len(fields)*2)
		for field, value := range fields {
			result = append(result, resp.NewBulkString(field))
			result = append(result, resp.NewBulkString(value))
		}
		return resp.NewArray(result), nil

	case "HLEN":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'hlen' command")), nil
		}

		key := value.Array[1].Str
		length, err := s.store.HLen(key)
		if err != nil {
			if err == store.ErrKeyNotFound || err == store.ErrKeyExpired {
				return resp.NewInteger(0), nil
			}
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}
		return resp.NewInteger(int64(length)), nil

	case "EXPIRE":
		if len(value.Array) != 3 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'expire' command")), nil
		}

		key := value.Array[1].Str
		seconds, err := strconv.Atoi(value.Array[2].Str)
		if err != nil {
			return resp.NewError(fmt.Errorf("ERR value is not an integer or out of range")), nil
		}

		ok := s.store.Expire(key, time.Duration(seconds)*time.Second)
		if ok {
			return resp.NewInteger(1), nil
		}
		return resp.NewInteger(0), nil

	case "TTL":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'ttl' command")), nil
		}

		key := value.Array[1].Str
		ttl := s.store.TTL(key)
		return resp.NewInteger(int64(ttl.Seconds())), nil

	case "KEYS":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'keys' command")), nil
		}

		pattern := value.Array[1].Str
		keys := s.store.Keys(pattern)
		result := make([]resp.Value, len(keys))
		for i, key := range keys {
			result[i] = resp.NewBulkString(key)
		}
		return resp.NewArray(result), nil

	default:
		return resp.NewError(fmt.Errorf("ERR unknown command '%s'", cmdName)), nil
	}
}
