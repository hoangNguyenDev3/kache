package store

import (
	"fmt"
	"strconv"
	"time"
)

func (s *Store) SetUnlocked(key string, value interface{}, expiry *time.Time) error {
	shard := s.getShard(key)
	shard.data[key] = &Entry{
		Value:     value,
		ExpiresAt: expiry,
	}
	s.logAOF([]string{"SET", key, fmt.Sprintf("%v", value)})
	return nil
}

func (s *Store) GetUnlocked(key string) (interface{}, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
		return nil, ErrKeyExpired
	}
	return entry.Value, nil
}

func (s *Store) IncrUnlocked(key string) (int64, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		shard.data[key] = &Entry{
			Value:     int64(1),
			ExpiresAt: nil,
		}
		s.logAOF([]string{"INCR", key})
		return 1, nil
	}
	if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
		delete(shard.data, key)
		shard.data[key] = &Entry{
			Value:     int64(1),
			ExpiresAt: nil,
		}
		s.logAOF([]string{"INCR", key})
		return 1, nil
	}
	switch v := entry.Value.(type) {
	case int64:
		newVal := v + 1
		entry.Value = newVal
		s.logAOF([]string{"INCR", key})
		return newVal, nil
	case int:
		newVal := int64(v) + 1
		entry.Value = newVal
		s.logAOF([]string{"INCR", key})
		return newVal, nil
	case string:
		var intVal int64
		if _, err := fmt.Sscanf(v, "%d", &intVal); err != nil {
			return 0, ErrWrongType
		}
		newVal := intVal + 1
		entry.Value = newVal
		s.logAOF([]string{"INCR", key})
		return newVal, nil
	default:
		return 0, ErrWrongType
	}
}

func (s *Store) DelUnlocked(keys ...string) (int, error) {
	count := 0
	for _, key := range keys {
		shard := s.getShard(key)
		if _, ok := shard.data[key]; ok {
			delete(shard.data, key)
			count++
			s.logAOF([]string{"DEL", key})
		}
	}
	return count, nil
}

func (s *Store) ExpireUnlocked(key string, duration time.Duration) bool {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return false
	}
	expiresAt := time.Now().Add(duration)
	entry.ExpiresAt = &expiresAt
	return true
}

func (s *Store) TTLUnlocked(key string) time.Duration {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return -2 * time.Second
	}
	if entry.ExpiresAt == nil {
		return -1 * time.Second
	}
	remaining := time.Until(*entry.ExpiresAt)
	if remaining < 0 {
		return -2 * time.Second
	}
	return remaining
}

func (s *Store) KeysUnlocked(pattern string) []string {
	keys := make([]string, 0)
	for _, shard := range s.shards {
		for k, entry := range shard.data {
			if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
				continue
			}
			keys = append(keys, k)
		}
	}
	return keys
}

// Hash unlocked methods

func (s *Store) HSetUnlocked(key, field, value string) (bool, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		hash := NewHash()
		hash.Fields[field] = value
		shard.data[key] = &Entry{
			Value: hash,
		}
		s.logAOF([]string{"HSET", key, field, value})
		return true, nil
	}
	hash, ok := entry.Value.(*Hash)
	if !ok {
		return false, ErrWrongType
	}
	_, exists := hash.Fields[field]
	hash.Fields[field] = value
	s.logAOF([]string{"HSET", key, field, value})
	return !exists, nil
}

func (s *Store) HGetUnlocked(key, field string) (string, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	hash, ok := entry.Value.(*Hash)
	if !ok {
		return "", ErrWrongType
	}
	value, ok := hash.Fields[field]
	if !ok {
		return "", ErrKeyNotFound
	}
	return value, nil
}

func (s *Store) HDelUnlocked(key string, fields ...string) (int, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return 0, nil
	}
	hash, ok := entry.Value.(*Hash)
	if !ok {
		return 0, ErrWrongType
	}
	count := 0
	for _, field := range fields {
		if _, ok := hash.Fields[field]; ok {
			delete(hash.Fields, field)
			s.logAOF([]string{"HDEL", key, field})
			count++
		}
	}
	return count, nil
}

func (s *Store) HGetAllUnlocked(key string) (map[string]string, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	hash, ok := entry.Value.(*Hash)
	if !ok {
		return nil, ErrWrongType
	}
	result := make(map[string]string, len(hash.Fields))
	for k, v := range hash.Fields {
		result[k] = v
	}
	return result, nil
}

func (s *Store) HLenUnlocked(key string) (int, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return 0, ErrKeyNotFound
	}
	hash, ok := entry.Value.(*Hash)
	if !ok {
		return 0, ErrWrongType
	}
	return len(hash.Fields), nil
}

// List unlocked methods

func (s *Store) LPushUnlocked(key string, values ...string) (int, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		list := NewList()
		for _, v := range values {
			list.elements = append([]string{v}, list.elements...)
		}
		shard.data[key] = &Entry{
			Value: list,
		}
		s.logAOF(append([]string{"LPUSH", key}, values...))
		return len(list.elements), nil
	}
	list, ok := entry.Value.(*List)
	if !ok {
		return 0, ErrWrongType
	}
	for _, v := range values {
		list.elements = append([]string{v}, list.elements...)
	}
	s.logAOF(append([]string{"LPUSH", key}, values...))
	return len(list.elements), nil
}

func (s *Store) RPushUnlocked(key string, values ...string) (int, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		list := NewList()
		list.elements = append(list.elements, values...)
		shard.data[key] = &Entry{
			Value: list,
		}
		s.logAOF(append([]string{"RPUSH", key}, values...))
		return len(list.elements), nil
	}
	list, ok := entry.Value.(*List)
	if !ok {
		return 0, ErrWrongType
	}
	list.elements = append(list.elements, values...)
	s.logAOF(append([]string{"RPUSH", key}, values...))
	return len(list.elements), nil
}

func (s *Store) LPopUnlocked(key string) (string, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	list, ok := entry.Value.(*List)
	if !ok {
		return "", ErrWrongType
	}
	if len(list.elements) == 0 {
		delete(shard.data, key)
		return "", ErrKeyNotFound
	}
	value := list.elements[0]
	list.elements = list.elements[1:]
	if len(list.elements) == 0 {
		delete(shard.data, key)
	}
	s.logAOF([]string{"LPOP", key})
	return value, nil
}

func (s *Store) RPopUnlocked(key string) (string, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	list, ok := entry.Value.(*List)
	if !ok {
		return "", ErrWrongType
	}
	if len(list.elements) == 0 {
		delete(shard.data, key)
		return "", ErrKeyNotFound
	}
	value := list.elements[len(list.elements)-1]
	list.elements = list.elements[:len(list.elements)-1]
	if len(list.elements) == 0 {
		delete(shard.data, key)
	}
	s.logAOF([]string{"RPOP", key})
	return value, nil
}

func (s *Store) LLenUnlocked(key string) (int, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return 0, nil
	}
	list, ok := entry.Value.(*List)
	if !ok {
		return 0, ErrWrongType
	}
	return len(list.elements), nil
}

func (s *Store) LRangeUnlocked(key string, start, stop int) ([]string, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return []string{}, nil
	}
	list, ok := entry.Value.(*List)
	if !ok {
		return nil, ErrWrongType
	}
	length := len(list.elements)
	if length == 0 {
		return []string{}, nil
	}
	start = normalizeIndex(start, length)
	stop = normalizeIndex(stop, length)
	start = clamp(start, length)
	stop = clamp(stop, length)
	if start > stop {
		return []string{}, nil
	}
	result := make([]string, stop-start+1)
	copy(result, list.elements[start:stop+1])
	return result, nil
}

func (s *Store) LIndexUnlocked(key string, index int) (string, error) {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	list, ok := entry.Value.(*List)
	if !ok {
		return "", ErrWrongType
	}
	length := len(list.elements)
	if length == 0 {
		return "", ErrKeyNotFound
	}
	index = normalizeIndex(index, length)
	if index < 0 || index >= length {
		return "", ErrKeyNotFound
	}
	return list.elements[index], nil
}

func (s *Store) LTrimUnlocked(key string, start, stop int) error {
	shard := s.getShard(key)
	entry, ok := shard.data[key]
	if !ok {
		return nil
	}
	list, ok := entry.Value.(*List)
	if !ok {
		return ErrWrongType
	}
	length := len(list.elements)
	if length == 0 {
		delete(shard.data, key)
		return nil
	}
	start = normalizeIndex(start, length)
	stop = normalizeIndex(stop, length)
	start = clamp(start, length)
	stop = clamp(stop, length)
	if start > stop {
		delete(shard.data, key)
		s.logAOF([]string{"LTRIM", key, strconv.Itoa(start), strconv.Itoa(stop)})
		return nil
	}
	list.elements = list.elements[start : stop+1]
	s.logAOF([]string{"LTRIM", key, strconv.Itoa(start), strconv.Itoa(stop)})
	return nil
}
