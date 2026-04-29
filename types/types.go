// Package types defines the core data types used throughout the Kache in-memory store.
package types

import (
	"time"
)

// Command represents a store operation with an optional expiration time.
type Command struct {
	Value     interface{} `json:"value,omitempty"`
	ExpiresAt *time.Time  `json:"expires_at,omitempty"`
	Operation string      `json:"op"`
	Key       string      `json:"key"`
	Field     string      `json:"field,omitempty"`
}

// Store defines the methods that must be implemented by a Kache store backend.
type Store interface {
	Set(key string, value interface{}, expiresAt *time.Time) error
	Get(key string) (interface{}, error)
	Del(keys ...string) (int, error)
	HSet(key, field, value string) (bool, error)
	HGet(key, field string) (string, error)
	HDel(key string, fields ...string) (int, error)
	HGetAll(key string) (map[string]string, error)
	HLen(key string) (int, error)
	Incr(key string) (int64, error)
	Expire(key string, duration time.Duration) bool
}

// Entry represents a value stored in the database with an optional expiration time.
type Entry struct {
	Value     interface{}
	ExpiresAt *time.Time
}
