package store

import (
	"sync"
)

// Hash represents a Redis hash data structure
type Hash struct {
	mu     sync.RWMutex `json:"-"`
	Fields map[string]string
}

// NewHash creates a new Hash instance
func NewHash() *Hash {
	return &Hash{
		Fields: make(map[string]string),
	}
}

// HSet sets field in the hash stored at key to value
func (s *Store) HSet(key, field, value string) (bool, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.data[key]
	if !ok {
		hash := NewHash()
		hash.Fields[field] = value
		shard.data[key] = &Entry{
			Value: hash,
		}

		if s.aof != nil {
			s.aof.LogOperation([]string{"HSET", key, field, value})
		}

		return true, nil
	}

	hash, ok := entry.Value.(*Hash)
	if !ok {
		return false, ErrWrongType
	}

	hash.mu.Lock()
	defer hash.mu.Unlock()

	_, exists := hash.Fields[field]
	hash.Fields[field] = value

	if s.aof != nil {
		s.aof.LogOperation([]string{"HSET", key, field, value})
	}

	return !exists, nil
}

// HGet returns the value associated with field in the hash stored at key
func (s *Store) HGet(key, field string) (string, error) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, ok := shard.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	hash, ok := entry.Value.(*Hash)
	if !ok {
		return "", ErrWrongType
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	value, ok := hash.Fields[field]
	if !ok {
		return "", ErrKeyNotFound
	}

	return value, nil
}

// HDel removes one or more fields from the hash stored at key
func (s *Store) HDel(key string, fields ...string) (int, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.data[key]
	if !ok {
		return 0, nil
	}

	hash, ok := entry.Value.(*Hash)
	if !ok {
		return 0, ErrWrongType
	}

	hash.mu.Lock()
	defer hash.mu.Unlock()

	count := 0
	for _, field := range fields {
		if _, ok := hash.Fields[field]; ok {
			delete(hash.Fields, field)
			if s.aof != nil {
				s.aof.LogOperation([]string{"HDEL", key, field})
			}
			count++
		}
	}

	return count, nil
}

// HGetAll returns all fields and values of the hash stored at key
func (s *Store) HGetAll(key string) (map[string]string, error) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, ok := shard.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	hash, ok := entry.Value.(*Hash)
	if !ok {
		return nil, ErrWrongType
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	result := make(map[string]string, len(hash.Fields))
	for k, v := range hash.Fields {
		result[k] = v
	}

	return result, nil
}

// HLen returns the number of fields in the hash stored at key
func (s *Store) HLen(key string) (int, error) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, ok := shard.data[key]
	if !ok {
		return 0, ErrKeyNotFound
	}

	hash, ok := entry.Value.(*Hash)
	if !ok {
		return 0, ErrWrongType
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	return len(hash.Fields), nil
}

// GetFields returns a copy of the hash fields
func (h *Hash) GetFields() map[string]string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	fields := make(map[string]string, len(h.Fields))
	for k, v := range h.Fields {
		fields[k] = v
	}
	return fields
}
