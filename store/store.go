package store

import (
	"fmt"
	"sync"
	"time"
)

// Create some error constants
var (
	ErrKeyNotFound = fmt.Errorf("key not found")
	ErrKeyExpired  = fmt.Errorf("key expired")
	ErrWrongType   = fmt.Errorf("wrong type")
	ErrWrongNode   = fmt.Errorf("wrong node")
)

// Entry represents a value stored in the database
type Entry struct {
	Value     interface{}
	ExpiresAt *time.Time
}

// Store holds the main data store
type Store struct {
	data map[string]*Entry
	mu   sync.RWMutex
	aof  *AOFWriter
}

// StoreConfig holds configuration for the store
type StoreConfig struct {
	AOFPath string
	RDBPath string
}

// New creates a new store
func New(config *StoreConfig) *Store {
	s := &Store{
		data: make(map[string]*Entry),
	}

	if config != nil && config.AOFPath != "" {
		// Enable AOF if a path is specified
		if err := s.EnableAOF(config.AOFPath); err != nil {
			fmt.Printf("Warning: Failed to enable AOF: %v\n", err)
		}
	}

	return s
}

// IsReplica returns whether this store is running in replica mode
func (s *Store) IsReplica() bool {
	return false
}

// Set sets a key with a value and optional expiry
func (s *Store) Set(key string, value interface{}, expiry *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = &Entry{
		Value:     value,
		ExpiresAt: expiry,
	}

	if s.aof != nil {
		s.aof.LogOperation([]string{"SET", key, fmt.Sprintf("%v", value)})
	}

	return nil
}

// Get retrieves a value by key
func (s *Store) Get(key string) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if key exists
	entry, ok := s.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Check expiry
	if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
		delete(s.data, key)
		return nil, ErrKeyExpired
	}

	return entry.Value, nil
}

// Incr increments a numeric key
func (s *Store) Incr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get existing value
	entry, ok := s.data[key]
	if !ok {
		// Key doesn't exist, initialize to 0
		s.data[key] = &Entry{
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
		delete(s.data, key)
		s.data[key] = &Entry{
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

// Del deletes one or more keys
func (s *Store) Del(keys ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, key := range keys {
		if _, ok := s.data[key]; ok {
			delete(s.data, key)
			count++

			if s.aof != nil {
				s.aof.LogOperation([]string{"DEL", key})
			}
		}
	}
	return count, nil
}

// Expire sets a timeout on a key
func (s *Store) Expire(key string, duration time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.data[key]
	if !ok {
		return false
	}

	expiresAt := time.Now().Add(duration)
	entry.ExpiresAt = &expiresAt
	return true
}

// TTL returns the remaining time to live of a key
func (s *Store) TTL(key string) time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[key]
	if !ok {
		return -2 * time.Second // Key does not exist
	}

	if entry.ExpiresAt == nil {
		return -1 * time.Second // Key exists but has no expiry
	}

	remaining := time.Until(*entry.ExpiresAt)
	if remaining < 0 {
		// Should remove the key but would require a write lock
		return -2 * time.Second
	}

	return remaining
}

// Keys returns all keys matching the given pattern
func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Simple implementation without pattern matching
	keys := make([]string, 0, len(s.data))
	for k, entry := range s.data {
		// Check expiry
		if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

// GC removes expired keys and returns the number of keys removed
func (s *Store) GC() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	removed := 0

	for key, entry := range s.data {
		if entry.ExpiresAt != nil && now.After(*entry.ExpiresAt) {
			delete(s.data, key)
			removed++
		}
	}

	return removed
}
