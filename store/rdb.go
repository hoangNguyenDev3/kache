package store

import (
	"encoding/gob"
	"fmt"
	"os"
	"time"
)

// RDBData represents the structure of the RDB file
type RDBData struct {
	Version int
	Data    map[string]RDBEntry
}

// RDBEntry represents an entry in the RDB file
type RDBEntry struct {
	Type      string
	Value     interface{}
	ExpiresAt *time.Time
}

// SaveRDB saves the current state to an RDB file
func (s *Store) SaveRDB(filePath string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create a new file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create RDB file: %w", err)
	}
	defer file.Close()

	// Create RDB data structure
	rdbData := RDBData{
		Version: 1,
		Data:    make(map[string]RDBEntry),
	}

	// Copy data to RDB structure
	now := time.Now()
	for key, entry := range s.data {
		// Skip expired keys
		if entry.ExpiresAt != nil && entry.ExpiresAt.Before(now) {
			continue
		}

		var entryType string
		switch entry.Value.(type) {
		case string:
			entryType = "string"
		case int64:
			entryType = "int"
		case map[string]string:
			entryType = "hash"
		default:
			// Skip unsupported types
			continue
		}

		rdbData.Data[key] = RDBEntry{
			Type:      entryType,
			Value:     entry.Value,
			ExpiresAt: entry.ExpiresAt,
		}
	}

	// Create an encoder and encode the data
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(rdbData); err != nil {
		return fmt.Errorf("failed to encode RDB data: %w", err)
	}

	fmt.Printf("Saved RDB to %s (%d keys)\n", filePath, len(rdbData.Data))
	return nil
}

// LoadRDB loads data from an RDB file
func (s *Store) LoadRDB(filePath string) error {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("RDB file does not exist: %s\n", filePath)
		return nil
	}

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	// Create a decoder
	decoder := gob.NewDecoder(file)

	// Decode the data
	var rdbData RDBData
	if err := decoder.Decode(&rdbData); err != nil {
		return fmt.Errorf("failed to decode RDB data: %w", err)
	}

	// Check version
	if rdbData.Version != 1 {
		return fmt.Errorf("unsupported RDB version: %d", rdbData.Version)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Load data into store
	now := time.Now()
	loaded := 0
	for key, entry := range rdbData.Data {
		// Skip expired keys
		if entry.ExpiresAt != nil && entry.ExpiresAt.Before(now) {
			continue
		}

		s.data[key] = &Entry{
			Value:     entry.Value,
			ExpiresAt: entry.ExpiresAt,
		}
		loaded++
	}

	fmt.Printf("Loaded %d keys from RDB file: %s\n", loaded, filePath)
	return nil
}
