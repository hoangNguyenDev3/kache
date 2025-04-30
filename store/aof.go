package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// AOFWriter handles append-only file operations
type AOFWriter struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	filename string
}

// NewAOFWriter creates a new AOF writer
func NewAOFWriter(filename string) (*AOFWriter, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open AOF file: %w", err)
	}

	return &AOFWriter{
		file:     file,
		writer:   bufio.NewWriter(file),
		filename: filename,
	}, nil
}

// LogOperation writes an operation to the AOF
func (w *AOFWriter) LogOperation(cmd []string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal operation: %w", err)
	}

	if _, err := w.writer.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write operation: %w", err)
	}

	return w.writer.Flush()
}

// Close closes the AOF writer
func (w *AOFWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// LoadAOF replays operations from the AOF file
func (s *Store) LoadAOF(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open AOF file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var cmd []string
		if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			return fmt.Errorf("failed to unmarshal operation: %w", err)
		}

		if len(cmd) == 0 {
			continue
		}

		switch cmd[0] {
		case "SET":
			if len(cmd) < 3 {
				continue
			}
			s.Set(cmd[1], cmd[2], nil)
		case "DEL":
			if len(cmd) < 2 {
				continue
			}
			s.Del(cmd[1])
		case "HSET":
			if len(cmd) < 4 {
				continue
			}
			s.HSet(cmd[1], cmd[2], cmd[3])
		case "HDEL":
			if len(cmd) < 3 {
				continue
			}
			s.HDel(cmd[1], cmd[2])
		case "INCR":
			if len(cmd) < 2 {
				continue
			}
			s.Incr(cmd[1])
		}
	}

	return scanner.Err()
}

// EnableAOF enables AOF persistence for the store
func (s *Store) EnableAOF(filename string) error {
	writer, err := NewAOFWriter(filename)
	if err != nil {
		return err
	}
	s.aof = writer
	return nil
}

// DisableAOF disables AOF persistence
func (s *Store) DisableAOF() error {
	if s.aof != nil {
		if err := s.aof.Close(); err != nil {
			return err
		}
		s.aof = nil
	}
	return nil
}
