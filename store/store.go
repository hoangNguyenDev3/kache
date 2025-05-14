// Package store implements a sharded concurrent in-memory key-value store with
// support for multiple data types, TTL-based expiration, and persistence.
//
// Concurrency model:
//   - The store is partitioned into 16 shards; each shard protects its map with
//     an independent sync.RWMutex. This design reduces lock contention under
//     high concurrency compared to a single global lock.
//   - Keys are assigned to shards using the FNV-1a 32-bit hash of the key
//     modulo NumShards.
//   - Expired keys are removed via a combination of lazy deletion on access
//     (Get, Incr) and a background active expiry goroutine that samples keys
//     in each shard.
//
// Persistence:
//   - RDB snapshots write the entire dataset to a custom binary file.
//   - AOF journaling logs every mutating command in RESP format.
package store

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hoangNguyenDev3/kache/resp"
)

// Exported error values returned by Store operations.
var (
	ErrKeyNotFound = fmt.Errorf("key not found")
	ErrKeyExpired  = fmt.Errorf("key expired")
	ErrWrongType   = fmt.Errorf("wrong type")
	ErrWrongNode   = fmt.Errorf("wrong node")
)

// NumShards is the number of shards in the store.
const NumShards = 16

// Entry represents a value stored in the database with an optional expiration time.
type Entry struct {
	Value     interface{}
	ExpiresAt *time.Time
}

// Shard holds a portion of the data store protected by its own RWMutex.
type Shard struct {
	data map[string]*Entry
	mu   sync.RWMutex
}

// Store represents a sharded concurrent in-memory key-value store.
// It supports strings, hashes, lists, TTL-based expiration, atomic
// increment, and optional AOF/RDB persistence.
type Store struct {
	shards [NumShards]*Shard
	aof    *AOFWriter
	config *StoreConfig
	done   chan struct{}
	rdbMu  sync.Mutex
	aofMu  sync.Mutex
}

// StoreConfig holds configuration for the store, including persistence paths
// and the interval between active expiry garbage-collection passes.
type StoreConfig struct {
	AOFPath    string
	RDBPath    string
	GCInterval time.Duration
}

// New creates and initializes a new Store with the given configuration.
// It allocates NumShards shards and starts a background active expiry
// goroutine when config is nil or GCInterval is non-negative.
func New(config *StoreConfig) *Store {
	s := &Store{
		config: config,
		done:   make(chan struct{}),
	}

	for i := 0; i < NumShards; i++ {
		s.shards[i] = &Shard{
			data: make(map[string]*Entry),
		}
	}

	if config == nil || config.GCInterval >= 0 {
		s.startGC()
	}

	return s
}

// getShard returns the shard for a given key using FNV-1a hashing.
// The hash is computed over the raw key bytes and reduced modulo NumShards
// to select the target shard. This keeps related keys colocated and
// distributes load evenly across shards.
func (s *Store) getShard(key string) *Shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	hashValue := h.Sum32()
	return s.shards[hashValue%NumShards]
}

// Set stores a key with the given value and optional expiry time.
// It is safe for concurrent use by multiple goroutines.
func (s *Store) Set(key string, value interface{}, expiry *time.Time) error {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.data[key] = &Entry{
		Value:     value,
		ExpiresAt: expiry,
	}

	if s.aof != nil {
		if expiry != nil {
			seconds := int(time.Until(*expiry).Seconds())
			if seconds < 1 {
				seconds = 1
			}
			s.aof.LogOperation([]string{"SET", key, fmt.Sprintf("%v", value), "EX", strconv.Itoa(seconds)})
		} else {
			s.aof.LogOperation([]string{"SET", key, fmt.Sprintf("%v", value)})
		}
	}

	return nil
}

// Get retrieves the value associated with key.
// It performs lazy deletion: if the key has expired, Get returns
// ErrKeyExpired without removing the key from the shard.
// It is safe for concurrent use by multiple goroutines.
func (s *Store) Get(key string) (interface{}, error) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// Check if key exists
	entry, ok := shard.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Check expiry — lazy deletion: return error but do not delete
	if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
		return nil, ErrKeyExpired
	}

	return entry.Value, nil
}

// Incr atomically increments the numeric value stored at key by one.
// If the key does not exist, it is initialized to 1. The operation is
// performed under the shard's write lock so it is safe for concurrent use.
func (s *Store) Incr(key string) (int64, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Get existing value
	entry, ok := shard.data[key]
	if !ok {
		// Key doesn't exist, initialize to 0
		shard.data[key] = &Entry{
			Value:     int64(1),
			ExpiresAt: nil,
		}

		if s.aof != nil {
			s.aof.LogOperation([]string{"INCR", key})
		}

		return 1, nil
	}

	// Check expiry
	if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
		delete(shard.data, key)
		shard.data[key] = &Entry{
			Value:     int64(1),
			ExpiresAt: nil,
		}

		if s.aof != nil {
			s.aof.LogOperation([]string{"INCR", key})
		}

		return 1, nil
	}

	// Check type and increment
	switch v := entry.Value.(type) {
	case int64:
		newVal := v + 1
		entry.Value = newVal

		if s.aof != nil {
			s.aof.LogOperation([]string{"INCR", key})
		}

		return newVal, nil
	case int:
		newVal := int64(v) + 1
		entry.Value = newVal

		if s.aof != nil {
			s.aof.LogOperation([]string{"INCR", key})
		}

		return newVal, nil
	case string:
		// Try to parse as integer
		var intVal int64
		if _, err := fmt.Sscanf(v, "%d", &intVal); err != nil {
			return 0, ErrWrongType
		}
		newVal := intVal + 1
		entry.Value = newVal

		if s.aof != nil {
			s.aof.LogOperation([]string{"INCR", key})
		}

		return newVal, nil
	default:
		return 0, ErrWrongType
	}
}

// Del deletes one or more keys and returns the number of keys removed.
// Each key is locked individually at the shard level; the operation is
// safe for concurrent use by multiple goroutines.
func (s *Store) Del(keys ...string) (int, error) {
	count := 0
	for _, key := range keys {
		shard := s.getShard(key)
		shard.mu.Lock()
		if _, ok := shard.data[key]; ok {
			delete(shard.data, key)
			count++

			if s.aof != nil {
				s.aof.LogOperation([]string{"DEL", key})
			}
		}
		shard.mu.Unlock()
	}
	return count, nil
}

// Expire sets a TTL on an existing key. It returns true if the timeout
// was set, or false if the key does not exist.
func (s *Store) Expire(key string, duration time.Duration) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.data[key]
	if !ok {
		return false
	}

	expiresAt := time.Now().Add(duration)
	entry.ExpiresAt = &expiresAt
	return true
}

// TTL returns the remaining time to live of a key.
// It returns -2s if the key does not exist, -1s if the key exists
// but has no expiry, and a positive duration otherwise.
func (s *Store) TTL(key string) time.Duration {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, ok := shard.data[key]
	if !ok {
		return -2 * time.Second // Key does not exist
	}

	if entry.ExpiresAt == nil {
		return -1 * time.Second // Key exists but has no expiry
	}

	remaining := time.Until(*entry.ExpiresAt)
	if remaining < 0 {
		return -2 * time.Second
	}

	return remaining
}

// Keys returns all non-expired keys in the store.
// The pattern argument is currently reserved; all keys are returned.
// It is safe for concurrent use because it acquires a read lock on
// each shard while iterating.
func (s *Store) Keys(pattern string) []string {
	keys := make([]string, 0)

	for _, shard := range s.shards {
		shard.mu.RLock()
		for k, entry := range shard.data {
			// Check expiry
			if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
				continue
			}
			keys = append(keys, k)
		}
		shard.mu.RUnlock()
	}

	return keys
}

// GC scans all shards and removes expired keys, returning the number
// of keys deleted. It acquires a write lock on each shard sequentially.
func (s *Store) GC() int {
	now := time.Now()
	removed := 0

	for _, shard := range s.shards {
		shard.mu.Lock()
		for key, entry := range shard.data {
			if entry.ExpiresAt != nil && now.After(*entry.ExpiresAt) {
				delete(shard.data, key)
				removed++
			}
		}
		shard.mu.Unlock()
	}

	return removed
}

// isExpired checks if an entry has expired
func isExpired(expiresAt *time.Time) bool {
	return expiresAt != nil && time.Now().After(*expiresAt)
}

func (s *Store) startGC() {
	interval := 100 * time.Millisecond
	if s.config != nil && s.config.GCInterval > 0 {
		interval = s.config.GCInterval
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				s.activeExpiry()
			}
		}
	}()
}

func (s *Store) activeExpiry() {
	for i := 0; i < NumShards; i++ {
		shard := s.shards[i]
		for {
			shard.mu.Lock()
			// Collect up to 20 keys that have expiry set
			sampled := 0
			expired := 0
			now := time.Now()
			for key, entry := range shard.data {
				if entry.ExpiresAt != nil {
					sampled++
					if now.After(*entry.ExpiresAt) {
						delete(shard.data, key)
						expired++
					}
				}
				if sampled >= 20 {
					break
				}
			}
			shard.mu.Unlock()

			// If less than 25% expired, move to next shard
			if sampled == 0 || float64(expired)/float64(sampled) < 0.25 {
				break
			}
			// Otherwise repeat this shard immediately
		}
	}
}

// Stop signals background goroutines to exit and closes the AOF writer.
func (s *Store) Stop() {
	close(s.done)
	if s.aof != nil {
		s.aof.Close()
	}
}

// SaveRDBBackground launches SaveRDB in a background goroutine.
// If a save is already in progress, the call is ignored.
func (s *Store) SaveRDBBackground() {
	go func() {
		if !s.rdbMu.TryLock() {
			return
		}
		defer s.rdbMu.Unlock()
		if s.config != nil && s.config.RDBPath != "" {
			s.SaveRDB(s.config.RDBPath)
		}
	}()
}

// RewriteAOF creates a new, compacted AOF file from the current dataset.
// It writes a temporary file, syncs it to disk, then atomically replaces
// the old AOF file via os.Rename. During the rewrite each shard is read
// locked individually to minimize write disruption.
func (s *Store) RewriteAOF() error {
	if s.aof == nil {
		return nil
	}

	tmpFile := s.aof.Filename() + ".tmp"
	f, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create temp AOF file: %w", err)
	}

	bw := bufio.NewWriter(f)

	for _, shard := range s.shards {
		shard.mu.RLock()
		for key, entry := range shard.data {
			if isExpired(entry.ExpiresAt) {
				continue
			}
			switch v := entry.Value.(type) {
			case string:
				cmd := []string{"SET", key, v}
				if entry.ExpiresAt != nil {
					seconds := int(time.Until(*entry.ExpiresAt).Seconds())
					if seconds < 1 {
						seconds = 1
					}
					cmd = append(cmd, "EX", strconv.Itoa(seconds))
				}
				bw.Write(resp.FormatCommand(cmd))
			case int64:
				cmd := []string{"SET", key, strconv.FormatInt(v, 10)}
				if entry.ExpiresAt != nil {
					seconds := int(time.Until(*entry.ExpiresAt).Seconds())
					if seconds < 1 {
						seconds = 1
					}
					cmd = append(cmd, "EX", strconv.Itoa(seconds))
				}
				bw.Write(resp.FormatCommand(cmd))
			case *Hash:
				fields := v.GetFields()
				for field, val := range fields {
					cmd := []string{"HSET", key, field, val}
					bw.Write(resp.FormatCommand(cmd))
				}
			case *List:
				elements := v.GetElements()
				if len(elements) > 0 {
					cmd := append([]string{"RPUSH", key}, elements...)
					bw.Write(resp.FormatCommand(cmd))
				}
			}
		}
		shard.mu.RUnlock()
	}

	if err := bw.Flush(); err != nil {
		f.Close()
		return fmt.Errorf("failed to flush temp AOF file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("failed to sync temp AOF file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close temp AOF file: %w", err)
	}

	// Close current AOF, rename, reopen
	if err := s.aof.Close(); err != nil {
		return fmt.Errorf("failed to close current AOF: %w", err)
	}

	if err := os.Rename(tmpFile, s.aof.Filename()); err != nil {
		return fmt.Errorf("failed to rename temp AOF file: %w", err)
	}

	newAof, err := NewAOFWriter(s.aof.Filename(), s.aof.FsyncPolicy())
	if err != nil {
		return fmt.Errorf("failed to reopen AOF file: %w", err)
	}
	s.aof = newAof
	return nil
}

// RewriteAOFBackground launches RewriteAOF in a background goroutine.
// If a rewrite is already in progress, the call is ignored.
func (s *Store) RewriteAOFBackground() {
	go func() {
		if !s.aofMu.TryLock() {
			return
		}
		defer s.aofMu.Unlock()
		s.RewriteAOF()
	}()
}

// LockAll acquires a write lock on every shard in ascending order.
// It is used internally to provide atomicity for MULTI/EXEC transactions.
func (s *Store) LockAll() {
	for i := 0; i < NumShards; i++ {
		s.shards[i].mu.Lock()
	}
}

// UnlockAll releases the write lock on every shard in descending order.
// It must be paired with a prior LockAll call.
func (s *Store) UnlockAll() {
	for i := NumShards - 1; i >= 0; i-- {
		s.shards[i].mu.Unlock()
	}
}
