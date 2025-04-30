package store

import (
	"encoding/gob"
	"fmt"
	"os"
	"time"
)

// RDBEntry represents an entry in the RDB file
type RDBEntry struct {
	Key       string
	Value     interface{}
	ExpiresAt *time.Time
}

// SaveRDB saves the current state to an RDB file
func (s *Store) SaveRDB(filename string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create RDB file: %w", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	// Register Hash type with gob
	gob.Register(&Hash{})

	entries := make([]RDBEntry, 0, len(s.data))
	for key, entry := range s.data {
		if entry.ExpiresAt != nil && isExpired(entry.ExpiresAt) {
			continue
		}
		entries = append(entries, RDBEntry{
			Key:       key,
			Value:     entry.Value,
			ExpiresAt: entry.ExpiresAt,
		})
	}

	if err := encoder.Encode(entries); err != nil {
		return fmt.Errorf("failed to encode RDB data: %w", err)
	}

	return nil
}

// LoadRDB loads the state from an RDB file
func (s *Store) LoadRDB(filename string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	var entries []RDBEntry
	if err := decoder.Decode(&entries); err != nil {
		return fmt.Errorf("failed to decode RDB data: %w", err)
	}

	// Clear current data
	s.data = make(map[string]*Entry)

	// Load entries
	for _, entry := range entries {
		if entry.ExpiresAt != nil && isExpired(entry.ExpiresAt) {
			continue
		}
		s.data[entry.Key] = &Entry{
			Value:     entry.Value,
			ExpiresAt: entry.ExpiresAt,
		}
	}

	return nil
}
