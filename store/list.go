package store

import (
	"encoding/json"
	"strconv"
	"sync"
)

// List represents a Redis-like list of strings. It is safe for concurrent use.
type List struct {
	elements []string
	mu       sync.RWMutex `json:"-" gob:"-"`
}

// GobEncode implements gob.GobEncoder, serializing the list elements as JSON.
func (l *List) GobEncode() ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return json.Marshal(l.elements)
}

// GobDecode implements gob.GobDecoder, deserializing the list elements from JSON.
func (l *List) GobDecode(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return json.Unmarshal(data, &l.elements)
}

// NewList creates and returns a new, empty List.
func NewList() *List {
	return &List{
		elements: make([]string, 0),
	}
}

// GetElements returns a copy of the list elements under a read lock.
// It is safe for concurrent use by multiple goroutines.
func (l *List) GetElements() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	elements := make([]string, len(l.elements))
	copy(elements, l.elements)
	return elements
}

// normalizeIndex converts a possibly-negative index to a positive one
func normalizeIndex(index, length int) int {
	if index < 0 {
		index = length + index
	}
	return index
}

// clamp clamps an index to the range [0, length-1]
func clamp(index, length int) int {
	if index < 0 {
		return 0
	}
	if index >= length {
		return length - 1
	}
	return index
}

// LPush prepends one or more values to the list stored at key. If the key
// does not exist, a new list is created. It returns the length of the list
// after the push. It is safe for concurrent use by multiple goroutines.
func (s *Store) LPush(key string, values ...string) (int, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.data[key]
	if !ok {
		list := NewList()
		// Prepend values so first arg ends up closest to head
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

	list.mu.Lock()
	defer list.mu.Unlock()

	// Prepend values so first arg ends up closest to head
	for _, v := range values {
		list.elements = append([]string{v}, list.elements...)
	}

	s.logAOF(append([]string{"LPUSH", key}, values...))

	return len(list.elements), nil
}

// RPush appends one or more values to the list stored at key. If the key
// does not exist, a new list is created. It returns the length of the list
// after the push. It is safe for concurrent use by multiple goroutines.
func (s *Store) RPush(key string, values ...string) (int, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

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

	list.mu.Lock()
	defer list.mu.Unlock()

	list.elements = append(list.elements, values...)

	s.logAOF(append([]string{"RPUSH", key}, values...))

	return len(list.elements), nil
}

// LPop removes and returns the first element of the list stored at key.
// If the list becomes empty, the key is deleted. It returns ErrKeyNotFound
// if the key does not exist or the list is empty.
// It is safe for concurrent use by multiple goroutines.
func (s *Store) LPop(key string) (string, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	list, ok := entry.Value.(*List)
	if !ok {
		return "", ErrWrongType
	}

	list.mu.Lock()
	defer list.mu.Unlock()

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

// RPop removes and returns the last element of the list stored at key.
// If the list becomes empty, the key is deleted. It returns ErrKeyNotFound
// if the key does not exist or the list is empty.
// It is safe for concurrent use by multiple goroutines.
func (s *Store) RPop(key string) (string, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	list, ok := entry.Value.(*List)
	if !ok {
		return "", ErrWrongType
	}

	list.mu.Lock()
	defer list.mu.Unlock()

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

// LLen returns the length of the list stored at key.
// It is safe for concurrent use by multiple goroutines.
func (s *Store) LLen(key string) (int, error) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, ok := shard.data[key]
	if !ok {
		return 0, nil
	}

	list, ok := entry.Value.(*List)
	if !ok {
		return 0, ErrWrongType
	}

	list.mu.RLock()
	defer list.mu.RUnlock()

	return len(list.elements), nil
}

// LRange returns the elements in the inclusive range [start, stop] from the
// list stored at key. Negative indices count from the end of the list.
// It is safe for concurrent use by multiple goroutines.
func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, ok := shard.data[key]
	if !ok {
		return []string{}, nil
	}

	list, ok := entry.Value.(*List)
	if !ok {
		return nil, ErrWrongType
	}

	list.mu.RLock()
	defer list.mu.RUnlock()

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

// LIndex returns the element at the given index in the list stored at key.
// Negative indices count from the end of the list. It returns ErrKeyNotFound
// if the key does not exist or the index is out of range.
// It is safe for concurrent use by multiple goroutines.
func (s *Store) LIndex(key string, index int) (string, error) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, ok := shard.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	list, ok := entry.Value.(*List)
	if !ok {
		return "", ErrWrongType
	}

	list.mu.RLock()
	defer list.mu.RUnlock()

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

// LTrim trims the list stored at key so that it contains only the elements
// in the inclusive range [start, stop]. If the range is empty, the key is
// deleted. Negative indices count from the end of the list.
// It is safe for concurrent use by multiple goroutines.
func (s *Store) LTrim(key string, start, stop int) error {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.data[key]
	if !ok {
		return nil
	}

	list, ok := entry.Value.(*List)
	if !ok {
		return ErrWrongType
	}

	list.mu.Lock()
	defer list.mu.Unlock()

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
