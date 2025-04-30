package store

import "time"

// HSet sets the specified field in the hash stored at key to value.
// Returns true if the field was added, false if it was updated.
func (s *Store) HSet(key, field, value string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get or create hash
	entry, ok := s.data[key]
	if !ok {
		// Create new hash
		hash := make(map[string]string)
		hash[field] = value
		s.data[key] = &Entry{
			Value:     hash,
			ExpiresAt: nil,
		}

		if s.aof != nil {
			s.aof.LogOperation([]string{"HSET", key, field, value})
		}

		return true, nil
	}

	// Check if entry is expired
	if entry.ExpiresAt != nil && s.isExpired(entry) {
		// Create new hash
		hash := make(map[string]string)
		hash[field] = value
		s.data[key] = &Entry{
			Value:     hash,
			ExpiresAt: nil,
		}

		if s.aof != nil {
			s.aof.LogOperation([]string{"HSET", key, field, value})
		}

		return true, nil
	}

	// Ensure value is a hash
	hash, ok := entry.Value.(map[string]string)
	if !ok {
		return false, ErrWrongType
	}

	// Check if field already exists
	_, exists := hash[field]

	// Set the field
	hash[field] = value

	if s.aof != nil {
		s.aof.LogOperation([]string{"HSET", key, field, value})
	}

	return !exists, nil
}

// HGet gets the value of the specified field in the hash stored at key.
func (s *Store) HGet(key, field string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get hash
	entry, ok := s.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	// Check if entry is expired
	if entry.ExpiresAt != nil && s.isExpired(entry) {
		delete(s.data, key)
		return "", ErrKeyExpired
	}

	// Ensure value is a hash
	hash, ok := entry.Value.(map[string]string)
	if !ok {
		return "", ErrWrongType
	}

	// Get field value
	value, ok := hash[field]
	if !ok {
		return "", ErrKeyNotFound
	}

	return value, nil
}

// HDel removes the specified fields from the hash stored at key.
// Returns the number of fields that were removed.
func (s *Store) HDel(key string, fields ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get hash
	entry, ok := s.data[key]
	if !ok {
		return 0, nil
	}

	// Check if entry is expired
	if entry.ExpiresAt != nil && s.isExpired(entry) {
		delete(s.data, key)
		return 0, nil
	}

	// Ensure value is a hash
	hash, ok := entry.Value.(map[string]string)
	if !ok {
		return 0, ErrWrongType
	}

	// Delete fields
	count := 0
	for _, field := range fields {
		if _, ok := hash[field]; ok {
			delete(hash, field)
			count++

			if s.aof != nil {
				s.aof.LogOperation([]string{"HDEL", key, field})
			}
		}
	}

	// If hash is now empty, remove the key
	if len(hash) == 0 {
		delete(s.data, key)
	}

	return count, nil
}

// HGetAll returns all fields and values of the hash stored at key.
func (s *Store) HGetAll(key string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get hash
	entry, ok := s.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Check if entry is expired
	if entry.ExpiresAt != nil && s.isExpired(entry) {
		delete(s.data, key)
		return nil, ErrKeyExpired
	}

	// Ensure value is a hash
	hash, ok := entry.Value.(map[string]string)
	if !ok {
		return nil, ErrWrongType
	}

	// Return a copy of the hash
	result := make(map[string]string, len(hash))
	for k, v := range hash {
		result[k] = v
	}

	return result, nil
}

// HLen returns the number of fields in the hash stored at key.
func (s *Store) HLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get hash
	entry, ok := s.data[key]
	if !ok {
		return 0, ErrKeyNotFound
	}

	// Check if entry is expired
	if entry.ExpiresAt != nil && s.isExpired(entry) {
		delete(s.data, key)
		return 0, ErrKeyExpired
	}

	// Ensure value is a hash
	hash, ok := entry.Value.(map[string]string)
	if !ok {
		return 0, ErrWrongType
	}

	return len(hash), nil
}

// HExists returns if field is an existing field in the hash stored at key.
func (s *Store) HExists(key, field string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get hash
	entry, ok := s.data[key]
	if !ok {
		return false, nil
	}

	// Check if entry is expired
	if entry.ExpiresAt != nil && s.isExpired(entry) {
		delete(s.data, key)
		return false, nil
	}

	// Ensure value is a hash
	hash, ok := entry.Value.(map[string]string)
	if !ok {
		return false, ErrWrongType
	}

	_, exists := hash[field]
	return exists, nil
}

// Helper method to check if an entry is expired
func (s *Store) isExpired(entry *Entry) bool {
	if entry.ExpiresAt == nil {
		return false
	}
	return entry.ExpiresAt.Before(s.now())
}

// Helper method to get current time (makes testing easier)
func (s *Store) now() time.Time {
	return time.Now()
}
