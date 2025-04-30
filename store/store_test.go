package store

import (
	"testing"
	"time"
)

func TestSet(t *testing.T) {
	s := New(nil)

	// Test setting a simple string value
	err := s.Set("key1", "value1", nil)
	if err != nil {
		t.Errorf("Failed to set key: %v", err)
	}

	val, err := s.Get("key1")
	if err != nil {
		t.Errorf("Failed to get key: %v", err)
	}
	if val != "value1" {
		t.Errorf("Expected 'value1', got '%v'", val)
	}

	// Test setting with expiry
	expiry := time.Now().Add(1 * time.Hour)
	err = s.Set("key2", "value2", &expiry)
	if err != nil {
		t.Errorf("Failed to set key with expiry: %v", err)
	}

	val, err = s.Get("key2")
	if err != nil {
		t.Errorf("Failed to get key with expiry: %v", err)
	}
	if val != "value2" {
		t.Errorf("Expected 'value2', got '%v'", val)
	}

	// Test overwriting a key
	err = s.Set("key1", "newvalue", nil)
	if err != nil {
		t.Errorf("Failed to overwrite key: %v", err)
	}

	val, err = s.Get("key1")
	if err != nil {
		t.Errorf("Failed to get overwritten key: %v", err)
	}
	if val != "newvalue" {
		t.Errorf("Expected 'newvalue', got '%v'", val)
	}
}

func TestGet(t *testing.T) {
	s := New(nil)

	// Test getting a non-existent key
	_, err := s.Get("nonexistent")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// Test getting a key after it expired
	expiry := time.Now().Add(-1 * time.Hour) // Expired 1 hour ago
	s.data["expired"] = &Entry{
		Value:     "expiredvalue",
		ExpiresAt: &expiry,
	}

	_, err = s.Get("expired")
	if err != ErrKeyExpired {
		t.Errorf("Expected ErrKeyExpired, got %v", err)
	}
}

func TestIncr(t *testing.T) {
	s := New(nil)

	// Test incrementing a non-existent key
	val, err := s.Incr("counter")
	if err != nil {
		t.Errorf("Failed to increment non-existent key: %v", err)
	}
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}

	// Test incrementing an existing key
	val, err = s.Incr("counter")
	if err != nil {
		t.Errorf("Failed to increment existing key: %v", err)
	}
	if val != 2 {
		t.Errorf("Expected 2, got %d", val)
	}

	// Test incrementing a non-numeric key
	s.data["string"] = &Entry{
		Value:     "not a number",
		ExpiresAt: nil,
	}

	_, err = s.Incr("string")
	if err != ErrWrongType {
		t.Errorf("Expected ErrWrongType, got %v", err)
	}
}

func TestDel(t *testing.T) {
	s := New(nil)

	// Set up some keys
	s.Set("key1", "value1", nil)
	s.Set("key2", "value2", nil)
	s.Set("key3", "value3", nil)

	// Test deleting a single key
	count, err := s.Del("key1")
	if err != nil {
		t.Errorf("Failed to delete key: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 key deleted, got %d", count)
	}

	// Test that the key was actually deleted
	_, err = s.Get("key1")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// Test deleting multiple keys
	count, err = s.Del("key2", "key3", "nonexistent")
	if err != nil {
		t.Errorf("Failed to delete keys: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 keys deleted, got %d", count)
	}
}

func TestExpire(t *testing.T) {
	s := New(nil)

	// Set up a key
	s.Set("key1", "value1", nil)

	// Test setting an expiry
	success := s.Expire("key1", 1*time.Hour)
	if !success {
		t.Errorf("Failed to set expiry")
	}

	// Check that TTL is set
	ttl := s.TTL("key1")
	if ttl.Seconds() <= 0 || ttl.Seconds() > 3600 {
		t.Errorf("Expected TTL between 0 and 3600 seconds, got %f", ttl.Seconds())
	}

	// Test setting expiry on a non-existent key
	success = s.Expire("nonexistent", 1*time.Hour)
	if success {
		t.Errorf("Expected false when setting expiry on non-existent key")
	}
}

func TestTTL(t *testing.T) {
	s := New(nil)

	// Test TTL on a non-existent key
	ttl := s.TTL("nonexistent")
	if ttl.Seconds() != -2 {
		t.Errorf("Expected -2 seconds for non-existent key, got %f", ttl.Seconds())
	}

	// Set up a key without expiry
	s.Set("key1", "value1", nil)

	// Test TTL on a key without expiry
	ttl = s.TTL("key1")
	if ttl.Seconds() != -1 {
		t.Errorf("Expected -1 seconds for key without expiry, got %f", ttl.Seconds())
	}

	// Set up a key with expiry
	expiry := time.Now().Add(1 * time.Hour)
	s.Set("key2", "value2", &expiry)

	// Test TTL on a key with expiry
	ttl = s.TTL("key2")
	if ttl.Seconds() <= 0 || ttl.Seconds() > 3600 {
		t.Errorf("Expected TTL between 0 and 3600 seconds, got %f", ttl.Seconds())
	}

	// Set up an expired key
	expiry = time.Now().Add(-1 * time.Hour)
	s.data["expired"] = &Entry{
		Value:     "expiredvalue",
		ExpiresAt: &expiry,
	}

	// Test TTL on an expired key
	ttl = s.TTL("expired")
	if ttl.Seconds() != -2 {
		t.Errorf("Expected -2 seconds for expired key, got %f", ttl.Seconds())
	}
}

func TestKeys(t *testing.T) {
	s := New(nil)

	// Test with empty store
	keys := s.Keys("*")
	if len(keys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(keys))
	}

	// Set up some keys
	s.Set("key1", "value1", nil)
	s.Set("key2", "value2", nil)
	s.Set("otherkey", "value3", nil)

	// Set up an expired key
	expiry := time.Now().Add(-1 * time.Hour)
	s.data["expired"] = &Entry{
		Value:     "expiredvalue",
		ExpiresAt: &expiry,
	}

	// Test getting all keys
	keys = s.Keys("*")
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	// Check that the expired key is not included
	for _, key := range keys {
		if key == "expired" {
			t.Errorf("Expired key should not be included in keys")
		}
	}
}

func TestGC(t *testing.T) {
	s := New(nil)

	// Set up some keys
	s.Set("key1", "value1", nil)
	s.Set("key2", "value2", nil)

	// Set up some expired keys
	expiry := time.Now().Add(-1 * time.Hour)
	s.data["expired1"] = &Entry{
		Value:     "expiredvalue1",
		ExpiresAt: &expiry,
	}
	s.data["expired2"] = &Entry{
		Value:     "expiredvalue2",
		ExpiresAt: &expiry,
	}

	// Run garbage collection
	count := s.GC()
	if count != 2 {
		t.Errorf("Expected 2 keys removed, got %d", count)
	}

	// Check that the expired keys are actually gone
	_, err1 := s.Get("expired1")
	_, err2 := s.Get("expired2")
	if err1 != ErrKeyNotFound || err2 != ErrKeyNotFound {
		t.Errorf("Expired keys should have been removed")
	}

	// Check that the non-expired keys are still there
	_, err1 = s.Get("key1")
	_, err2 = s.Get("key2")
	if err1 != nil || err2 != nil {
		t.Errorf("Non-expired keys should not have been removed")
	}
}

func TestSize(t *testing.T) {
	s := New(nil)

	// Test with empty store
	size := s.Size()
	if size != 0 {
		t.Errorf("Expected size 0, got %d", size)
	}

	// Add some keys
	s.Set("key1", "value1", nil)
	s.Set("key2", "value2", nil)

	// Test with non-empty store
	size = s.Size()
	if size != 2 {
		t.Errorf("Expected size 2, got %d", size)
	}

	// Delete a key
	s.Del("key1")

	// Test after deletion
	size = s.Size()
	if size != 1 {
		t.Errorf("Expected size 1, got %d", size)
	}
}
