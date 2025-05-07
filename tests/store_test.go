package test

import (
	"testing"
	"time"

	"github.com/hoangNguyenDev3/kache/store"
	"github.com/stretchr/testify/assert"
)

func TestStore_Set(t *testing.T) {
	s := store.New(&store.StoreConfig{GCInterval: -1})

	// Test simple set
	err := s.Set("key1", "value1", nil)
	assert.NoError(t, err)
	value, err := s.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)

	// Test set with expiration
	expiration := time.Now().Add(100 * time.Millisecond)
	err = s.Set("key2", "value2", &expiration)
	assert.NoError(t, err)
	value, err = s.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, "value2", value)

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)
	value, err = s.Get("key2")
	assert.Equal(t, store.ErrKeyExpired, err)
	assert.Nil(t, value)
}

func TestStore_Del(t *testing.T) {
	s := store.New(nil)

	// Set some keys
	err := s.Set("key1", "value1", nil)
	assert.NoError(t, err)
	err = s.Set("key2", "value2", nil)
	assert.NoError(t, err)

	// Delete one key
	count, err := s.Del("key1")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
	_, err = s.Get("key1")
	assert.Equal(t, store.ErrKeyNotFound, err)

	// Delete multiple keys
	err = s.Set("key1", "value1", nil)
	assert.NoError(t, err)
	count, err = s.Del("key1", "key2", "key3")
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
	_, err = s.Get("key1")
	assert.Equal(t, store.ErrKeyNotFound, err)
	_, err = s.Get("key2")
	assert.Equal(t, store.ErrKeyNotFound, err)
}

func TestStore_Hash(t *testing.T) {
	s := store.New(nil)

	// Test HSET
	created, err := s.HSet("hash1", "field1", "value1")
	assert.NoError(t, err)
	assert.True(t, created)

	// Test HGET
	value, err := s.HGet("hash1", "field1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)

	// Test HSET update
	created, err = s.HSet("hash1", "field1", "value2")
	assert.NoError(t, err)
	assert.False(t, created)
	value, err = s.HGet("hash1", "field1")
	assert.NoError(t, err)
	assert.Equal(t, "value2", value)

	// Test HDEL
	count, err := s.HDel("hash1", "field1")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
	_, err = s.HGet("hash1", "field1")
	assert.Equal(t, store.ErrKeyNotFound, err)

	// Test HGETALL
	_, err = s.HSet("hash2", "field1", "value1")
	assert.NoError(t, err)
	_, err = s.HSet("hash2", "field2", "value2")
	assert.NoError(t, err)
	fields, err := s.HGetAll("hash2")
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{
		"field1": "value1",
		"field2": "value2",
	}, fields)

	// Test HLEN
	length, err := s.HLen("hash2")
	assert.NoError(t, err)
	assert.Equal(t, 2, length)
}

func TestStore_WrongType(t *testing.T) {
	s := store.New(nil)

	// Set a string value
	err := s.Set("key", "value", nil)
	assert.NoError(t, err)

	// Try to increment a string value
	_, err = s.Incr("key")
	assert.Error(t, err)
	assert.Equal(t, store.ErrWrongType, err)
}

func TestStore_GC(t *testing.T) {
	s := store.New(&store.StoreConfig{GCInterval: -1})

	// Set some keys with expiration
	expiration := time.Now().Add(100 * time.Millisecond)
	err := s.Set("key1", "value1", &expiration)
	assert.NoError(t, err)
	err = s.Set("key2", "value2", &expiration)
	assert.NoError(t, err)
	err = s.Set("key3", "value3", nil)
	assert.NoError(t, err)

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	// Run garbage collection
	removed := s.GC()
	assert.Equal(t, 2, removed)

	// Check that expired keys are gone
	_, err = s.Get("key1")
	assert.Equal(t, store.ErrKeyNotFound, err)
	_, err = s.Get("key2")
	assert.Equal(t, store.ErrKeyNotFound, err)

	// Check that non-expired key is still there
	value, err := s.Get("key3")
	assert.NoError(t, err)
	assert.Equal(t, "value3", value)
}

func BenchmarkStore_Set(b *testing.B) {
	s := store.New(nil)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "key" + string(rune(i))
		s.Set(key, "value", nil)
	}
}

func BenchmarkStore_Get(b *testing.B) {
	s := store.New(nil)
	s.Set("key", "value", nil)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get("key")
	}
}

func BenchmarkStore_HSet(b *testing.B) {
	s := store.New(nil)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		field := "field" + string(rune(i))
		s.HSet("hash", field, "value")
	}
}

func BenchmarkStore_HGet(b *testing.B) {
	s := store.New(nil)
	s.HSet("hash", "field", "value")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.HGet("hash", "field")
	}
}
