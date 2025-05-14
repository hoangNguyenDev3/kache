// Package server implements TCP (RESP protocol) and HTTP/REST API servers
// for the Kache data store. The TCP server handles Redis-compatible commands
// including strings, hashes, lists, pub/sub, and MULTI/EXEC transactions.
package server

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hoangNguyenDev3/kache/pubsub"
	"github.com/hoangNguyenDev3/kache/resp"
	"github.com/hoangNguyenDev3/kache/store"
)

// Client represents a connected TCP client and its session state.
type Client struct {
	conn          net.Conn
	server        *TCPServer
	lastSeen      time.Time
	inTransaction bool
	txQueue       []resp.Value
}

// TCPServer accepts TCP connections and processes RESP protocol commands.
// It supports pipelining, transactions, pub/sub, and automatic client timeouts.
type TCPServer struct {
	listener net.Listener
	store    *store.Store
	clients  sync.Map
	config   *Config
	done     chan struct{}
	pubsub   *pubsub.PubSub
}

// Config holds TCP server configuration.
type Config struct {
	ClientTimeout time.Duration
	TLSConfig     *tls.Config
}

// NewTCPServer creates and returns a new TCPServer backed by the given store.
// If ps is nil, a new PubSub instance is created automatically.
func NewTCPServer(store *store.Store, config *Config, ps *pubsub.PubSub) *TCPServer {
	if ps == nil {
		ps = pubsub.New()
	}
	return &TCPServer{
		store:  store,
		config: config,
		done:   make(chan struct{}),
		pubsub: ps,
	}
}

// Start binds the TCP server to addr and begins accepting connections.
// It returns immediately; connections are handled in background goroutines.
func (s *TCPServer) Start(addr string) error {
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start TCP server: %w", err)
	}

	if s.config.TLSConfig != nil {
		s.listener = tls.NewListener(s.listener, s.config.TLSConfig)
		slog.Info("tcp server listening with TLS", "addr", addr, "component", "tcp")
	} else {
		slog.Info("tcp server listening", "addr", addr, "component", "tcp")
	}

	go s.acceptConnections()
	return nil
}

// Stop signals the server to shut down, closes all client connections,
// and releases the listener.
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
					slog.Error("failed to accept connection", "error", err, "component", "tcp")
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
	writer := bufio.NewWriter(conn)
	defer writer.Flush()

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
				if err := resp.WriteError(writer, fmt.Sprintf("ERR %v", err)); err != nil {
					writer.Flush()
					return
				}
				if reader.Buffered() == 0 {
					writer.Flush()
				}
				continue
			}

			// Get command name for transaction checks
			cmdName := ""
			if value.Type == resp.Array && len(value.Array) > 0 {
				cmdName = strings.ToUpper(value.Array[0].Str)
			}

			// Handle SUBSCRIBE specially - enters subscription mode
			if cmdName == "SUBSCRIBE" {
				if err := s.handleSubscribe(conn, reader, writer, value, client); err != nil {
					return
				}
				continue
			}

			// Transaction command queuing
			if client.inTransaction && cmdName != "EXEC" && cmdName != "DISCARD" && cmdName != "MULTI" {
				client.txQueue = append(client.txQueue, value)
				queuedResponse := resp.NewSimpleString("QUEUED")
				if err := resp.Write(writer, &queuedResponse); err != nil {
					writer.Flush()
					return
				}
				if reader.Buffered() == 0 {
					writer.Flush()
				}
				client.lastSeen = time.Now()
				continue
			}

			var response resp.Value

			switch cmdName {
			case "MULTI":
				if client.inTransaction {
					response = resp.NewError(fmt.Errorf("ERR MULTI calls can not be nested"))
				} else {
					client.inTransaction = true
					client.txQueue = make([]resp.Value, 0)
					response = resp.NewSimpleString("OK")
				}
			case "EXEC":
				if !client.inTransaction {
					response = resp.NewError(fmt.Errorf("ERR EXEC without MULTI"))
				} else {
					s.store.LockAll()
					results := make([]resp.Value, 0, len(client.txQueue))
					for _, queuedValue := range client.txQueue {
						res, _ := s.processCommand(queuedValue, &unlockedStore{store: s.store})
						results = append(results, res)
					}
					s.store.UnlockAll()
					client.inTransaction = false
					client.txQueue = nil
					response = resp.NewArray(results)
				}
			case "DISCARD":
				if !client.inTransaction {
					response = resp.NewError(fmt.Errorf("ERR DISCARD without MULTI"))
				} else {
					client.inTransaction = false
					client.txQueue = nil
					response = resp.NewSimpleString("OK")
				}
			case "PUBLISH":
				if len(value.Array) != 3 {
					response = resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'publish' command"))
				} else {
					channel := value.Array[1].Str
					message := value.Array[2].Str
					count := s.pubsub.Publish(channel, message)
					response = resp.NewInteger(int64(count))
				}
			default:
				var procErr error
				response, procErr = s.processCommand(value, s.store)
				if procErr != nil {
					if err := resp.WriteError(writer, fmt.Sprintf("ERR %v", procErr)); err != nil {
						writer.Flush()
						return
					}
					if reader.Buffered() == 0 {
						writer.Flush()
					}
					continue
				}
			}

			// Send response
			if err := resp.Write(writer, &response); err != nil {
				writer.Flush()
				return
			}

			// Pipelining: flush only when no more buffered commands
			if reader.Buffered() == 0 {
				writer.Flush()
			}

			// Update last seen time
			client.lastSeen = time.Now()
		}
	}
}

func (s *TCPServer) handleSubscribe(conn net.Conn, reader *bufio.Reader, writer *bufio.Writer, value resp.Value, client *Client) error {
	if len(value.Array) < 2 {
		errResp := resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'subscribe' command"))
		if err := resp.Write(writer, &errResp); err != nil {
			return err
		}
		writer.Flush()
		return nil
	}

	channels := make([]string, len(value.Array)-1)
	for i := 1; i < len(value.Array); i++ {
		channels[i-1] = value.Array[i].Str
	}

	subscriberID := conn.RemoteAddr().String()
	subscriber := s.pubsub.Subscribe(subscriberID, channels...)

	// Send confirmation for each channel
	for i, ch := range channels {
		count := i + 1
		confirmation := resp.NewArray([]resp.Value{
			resp.NewBulkString("subscribe"),
			resp.NewBulkString(ch),
			resp.NewInteger(int64(count)),
		})
		if err := resp.Write(writer, &confirmation); err != nil {
			return err
		}
	}
	writer.Flush()

	// Track subscribed channels locally
	subscribedChannels := make(map[string]bool)
	for _, ch := range channels {
		subscribedChannels[ch] = true
	}

	// Start goroutine to read commands from connection
	cmdChan := make(chan resp.Value, 1)
	goroutineDone := make(chan struct{})
	stopChan := make(chan struct{})

	var stopOnce sync.Once
	stopGoroutine := func() {
		stopOnce.Do(func() {
			close(stopChan)
			conn.SetReadDeadline(time.Now())
			<-goroutineDone
			conn.SetReadDeadline(time.Time{})
		})
	}
	defer stopGoroutine()

	go func() {
		defer close(goroutineDone)
		defer close(cmdChan)
		for {
			value, err := resp.Parse(reader)
			if err != nil {
				return
			}
			select {
			case cmdChan <- value:
			case <-stopChan:
				return
			case <-s.done:
				return
			}
		}
	}()

	// Subscription loop
	for {
		select {
		case msg := <-subscriber.Messages:
			push := resp.NewArray([]resp.Value{
				resp.NewBulkString("message"),
				resp.NewBulkString(msg.Channel),
				resp.NewBulkString(fmt.Sprintf("%v", msg.Data)),
			})
			if err := resp.Write(writer, &push); err != nil {
				return err
			}
			writer.Flush()

		case <-subscriber.Done:
			return nil

		case <-s.done:
			return nil

		case <-goroutineDone:
			return nil

		case value, ok := <-cmdChan:
			if !ok {
				return nil
			}

			if value.Type != resp.Array || len(value.Array) == 0 {
				continue
			}

			cmdName := strings.ToUpper(value.Array[0].Str)
			switch cmdName {
			case "UNSUBSCRIBE":
				unsubChannels := make([]string, 0)
				if len(value.Array) == 1 {
					// Unsubscribe from all channels
					for ch := range subscribedChannels {
						unsubChannels = append(unsubChannels, ch)
					}
				} else {
					for i := 1; i < len(value.Array); i++ {
						unsubChannels = append(unsubChannels, value.Array[i].Str)
					}
				}

				s.pubsub.Unsubscribe(subscriberID, unsubChannels...)
				for _, ch := range unsubChannels {
					delete(subscribedChannels, ch)
				}

				remaining := len(subscribedChannels)
				for _, ch := range unsubChannels {
					confirmation := resp.NewArray([]resp.Value{
						resp.NewBulkString("unsubscribe"),
						resp.NewBulkString(ch),
						resp.NewInteger(int64(remaining)),
					})
					if err := resp.Write(writer, &confirmation); err != nil {
						return err
					}
				}
				writer.Flush()

				if len(subscribedChannels) == 0 {
					return nil
				}

			case "SUBSCRIBE":
				// Additional subscriptions
				newChannels := make([]string, len(value.Array)-1)
				for i := 1; i < len(value.Array); i++ {
					newChannels[i-1] = value.Array[i].Str
				}
				s.pubsub.Subscribe(subscriberID, newChannels...)
				for _, ch := range newChannels {
					subscribedChannels[ch] = true
				}
				for i, ch := range newChannels {
					count := len(subscribedChannels) - len(newChannels) + i + 1
					confirmation := resp.NewArray([]resp.Value{
						resp.NewBulkString("subscribe"),
						resp.NewBulkString(ch),
						resp.NewInteger(int64(count)),
					})
					if err := resp.Write(writer, &confirmation); err != nil {
						return err
					}
				}
				writer.Flush()
			}
		}
	}
}

type storeOps interface {
	Set(key string, value interface{}, expiry *time.Time) error
	Get(key string) (interface{}, error)
	Incr(key string) (int64, error)
	Del(keys ...string) (int, error)
	HSet(key, field, value string) (bool, error)
	HGet(key, field string) (string, error)
	HDel(key string, fields ...string) (int, error)
	HGetAll(key string) (map[string]string, error)
	HLen(key string) (int, error)
	Expire(key string, duration time.Duration) bool
	TTL(key string) time.Duration
	Keys(pattern string) []string
	LPush(key string, values ...string) (int, error)
	RPush(key string, values ...string) (int, error)
	LPop(key string) (string, error)
	RPop(key string) (string, error)
	LLen(key string) (int, error)
	LRange(key string, start, stop int) ([]string, error)
	LIndex(key string, index int) (string, error)
	LTrim(key string, start, stop int) error
}

type unlockedStore struct {
	store *store.Store
}

func (u *unlockedStore) Set(key string, value interface{}, expiry *time.Time) error {
	return u.store.SetUnlocked(key, value, expiry)
}
func (u *unlockedStore) Get(key string) (interface{}, error) {
	return u.store.GetUnlocked(key)
}
func (u *unlockedStore) Incr(key string) (int64, error) {
	return u.store.IncrUnlocked(key)
}
func (u *unlockedStore) Del(keys ...string) (int, error) {
	return u.store.DelUnlocked(keys...)
}
func (u *unlockedStore) HSet(key, field, value string) (bool, error) {
	return u.store.HSetUnlocked(key, field, value)
}
func (u *unlockedStore) HGet(key, field string) (string, error) {
	return u.store.HGetUnlocked(key, field)
}
func (u *unlockedStore) HDel(key string, fields ...string) (int, error) {
	return u.store.HDelUnlocked(key, fields...)
}
func (u *unlockedStore) HGetAll(key string) (map[string]string, error) {
	return u.store.HGetAllUnlocked(key)
}
func (u *unlockedStore) HLen(key string) (int, error) {
	return u.store.HLenUnlocked(key)
}
func (u *unlockedStore) Expire(key string, duration time.Duration) bool {
	return u.store.ExpireUnlocked(key, duration)
}
func (u *unlockedStore) TTL(key string) time.Duration {
	return u.store.TTLUnlocked(key)
}
func (u *unlockedStore) Keys(pattern string) []string {
	return u.store.KeysUnlocked(pattern)
}
func (u *unlockedStore) LPush(key string, values ...string) (int, error) {
	return u.store.LPushUnlocked(key, values...)
}
func (u *unlockedStore) RPush(key string, values ...string) (int, error) {
	return u.store.RPushUnlocked(key, values...)
}
func (u *unlockedStore) LPop(key string) (string, error) {
	return u.store.LPopUnlocked(key)
}
func (u *unlockedStore) RPop(key string) (string, error) {
	return u.store.RPopUnlocked(key)
}
func (u *unlockedStore) LLen(key string) (int, error) {
	return u.store.LLenUnlocked(key)
}
func (u *unlockedStore) LRange(key string, start, stop int) ([]string, error) {
	return u.store.LRangeUnlocked(key, start, stop)
}
func (u *unlockedStore) LIndex(key string, index int) (string, error) {
	return u.store.LIndexUnlocked(key, index)
}
func (u *unlockedStore) LTrim(key string, start, stop int) error {
	return u.store.LTrimUnlocked(key, start, stop)
}

func (s *TCPServer) processCommand(value resp.Value, st storeOps) (resp.Value, error) {
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

		if err := st.Set(key, val, expiry); err != nil {
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

		val, err := st.Get(key)
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
		val, err := st.Incr(key)
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

		count, err := st.Del(keys...)
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

		created, err := st.HSet(key, field, val)
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

		val, err := st.HGet(key, field)
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

		count, err := st.HDel(key, fields...)
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
		fields, err := st.HGetAll(key)
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
		length, err := st.HLen(key)
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

		ok := st.Expire(key, time.Duration(seconds)*time.Second)
		if ok {
			return resp.NewInteger(1), nil
		}
		return resp.NewInteger(0), nil

	case "TTL":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'ttl' command")), nil
		}

		key := value.Array[1].Str
		ttl := st.TTL(key)
		return resp.NewInteger(int64(ttl.Seconds())), nil

	case "KEYS":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'keys' command")), nil
		}

		pattern := value.Array[1].Str
		keys := st.Keys(pattern)
		result := make([]resp.Value, len(keys))
		for i, key := range keys {
			result[i] = resp.NewBulkString(key)
		}
		return resp.NewArray(result), nil

	case "LPUSH":
		if len(value.Array) < 3 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'lpush' command")), nil
		}

		key := value.Array[1].Str
		values := make([]string, len(value.Array)-2)
		for i := 2; i < len(value.Array); i++ {
			values[i-2] = value.Array[i].Str
		}

		length, err := st.LPush(key, values...)
		if err != nil {
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}
		return resp.NewInteger(int64(length)), nil

	case "RPUSH":
		if len(value.Array) < 3 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'rpush' command")), nil
		}

		key := value.Array[1].Str
		values := make([]string, len(value.Array)-2)
		for i := 2; i < len(value.Array); i++ {
			values[i-2] = value.Array[i].Str
		}

		length, err := st.RPush(key, values...)
		if err != nil {
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}
		return resp.NewInteger(int64(length)), nil

	case "LPOP":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'lpop' command")), nil
		}

		key := value.Array[1].Str
		val, err := st.LPop(key)
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

	case "RPOP":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'rpop' command")), nil
		}

		key := value.Array[1].Str
		val, err := st.RPop(key)
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

	case "LLEN":
		if len(value.Array) != 2 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'llen' command")), nil
		}

		key := value.Array[1].Str
		length, err := st.LLen(key)
		if err != nil {
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}
		return resp.NewInteger(int64(length)), nil

	case "LRANGE":
		if len(value.Array) != 4 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'lrange' command")), nil
		}

		key := value.Array[1].Str
		start, err := strconv.Atoi(value.Array[2].Str)
		if err != nil {
			return resp.NewError(fmt.Errorf("ERR value is not an integer or out of range")), nil
		}
		stop, err := strconv.Atoi(value.Array[3].Str)
		if err != nil {
			return resp.NewError(fmt.Errorf("ERR value is not an integer or out of range")), nil
		}

		values, err := st.LRange(key, start, stop)
		if err != nil {
			if err == store.ErrKeyNotFound || err == store.ErrKeyExpired {
				return resp.NewArray([]resp.Value{}), nil
			}
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}

		result := make([]resp.Value, len(values))
		for i, v := range values {
			result[i] = resp.NewBulkString(v)
		}
		return resp.NewArray(result), nil

	case "LINDEX":
		if len(value.Array) != 3 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'lindex' command")), nil
		}

		key := value.Array[1].Str
		index, err := strconv.Atoi(value.Array[2].Str)
		if err != nil {
			return resp.NewError(fmt.Errorf("ERR value is not an integer or out of range")), nil
		}

		val, err := st.LIndex(key, index)
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

	case "BGSAVE":
		s.store.SaveRDBBackground()
		return resp.NewSimpleString("Background saving started"), nil

	case "BGREWRITEAOF":
		s.store.RewriteAOFBackground()
		return resp.NewSimpleString("Background append only file rewriting started"), nil

	case "LTRIM":
		if len(value.Array) != 4 {
			return resp.NewError(fmt.Errorf("ERR wrong number of arguments for 'ltrim' command")), nil
		}

		key := value.Array[1].Str
		start, err := strconv.Atoi(value.Array[2].Str)
		if err != nil {
			return resp.NewError(fmt.Errorf("ERR value is not an integer or out of range")), nil
		}
		stop, err := strconv.Atoi(value.Array[3].Str)
		if err != nil {
			return resp.NewError(fmt.Errorf("ERR value is not an integer or out of range")), nil
		}

		if err := st.LTrim(key, start, stop); err != nil {
			if err == store.ErrWrongNode {
				return resp.NewError(fmt.Errorf("MOVED")), nil
			}
			return resp.NewError(err), nil
		}
		return resp.NewSimpleString("OK"), nil

	default:
		return resp.NewError(fmt.Errorf("ERR unknown command '%s'", cmdName)), nil
	}
}
