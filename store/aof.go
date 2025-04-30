package store

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hoangNguyenDev3/redis-clone/resp"
)

// AOFWriter handles append-only file operations
type AOFWriter struct {
	mu       sync.Mutex
	file     *os.File
	filePath string
	writer   *bufio.Writer
}

// EnableAOF enables AOF persistence for a store
func (s *Store) EnableAOF(filePath string) error {
	// Open or create the AOF file
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open AOF file: %w", err)
	}

	// Create a buffered writer
	writer := bufio.NewWriter(file)

	// Set up the AOF writer
	s.aof = &AOFWriter{
		file:     file,
		filePath: filePath,
		writer:   writer,
	}

	fmt.Printf("AOF persistence enabled: %s\n", filePath)
	return nil
}

// LogOperation logs a command to the AOF file
func (a *AOFWriter) LogOperation(cmd []string) error {
	if len(cmd) == 0 {
		return fmt.Errorf("empty command")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Format command as RESP
	cmdBytes := resp.FormatCommand(cmd)

	// Write to file
	if _, err := a.writer.Write(cmdBytes); err != nil {
		return fmt.Errorf("failed to write command to AOF: %w", err)
	}

	// Flush to disk (can be optimized for better performance)
	if err := a.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush AOF buffer: %w", err)
	}

	return nil
}

// LoadAOF loads data from an AOF file
func (s *Store) LoadAOF(filePath string) error {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("AOF file does not exist: %s\n", filePath)
		return nil
	}

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open AOF file: %w", err)
	}
	defer file.Close()

	fmt.Printf("Loading AOF file: %s\n", filePath)

	// Create a reader
	reader := bufio.NewReader(file)

	// Process each command
	count := 0
	for {
		value, err := resp.Parse(reader)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Printf("Warning: Error parsing AOF entry: %v\n", err)
			continue
		}

		if value.Type != resp.Array {
			fmt.Printf("Warning: Invalid AOF entry (not an array)\n")
			continue
		}

		// Convert to command strings
		cmd := make([]string, len(value.Array))
		for i, v := range value.Array {
			if v.Type != resp.BulkString {
				fmt.Printf("Warning: Invalid AOF command argument (not a bulk string)\n")
				continue
			}
			cmd[i] = v.Str
		}

		// Execute the command
		if len(cmd) > 0 {
			if err := s.execAOFCommand(cmd); err != nil {
				fmt.Printf("Warning: Failed to execute AOF command: %v\n", err)
				continue
			}
			count++
		}
	}

	fmt.Printf("Loaded %d commands from AOF\n", count)
	return nil
}

// execAOFCommand executes a command loaded from AOF
func (s *Store) execAOFCommand(cmd []string) error {
	if len(cmd) == 0 {
		return fmt.Errorf("empty command")
	}

	// Convert command to uppercase for case-insensitive matching
	command := strings.ToUpper(cmd[0])

	switch command {
	case "SET":
		if len(cmd) < 3 {
			return fmt.Errorf("invalid SET command")
		}
		key := cmd[1]
		value := cmd[2]

		var expiry *time.Time
		if len(cmd) >= 5 && strings.ToUpper(cmd[3]) == "EX" {
			seconds, err := parseInt(cmd[4])
			if err != nil {
				return fmt.Errorf("invalid expiry: %w", err)
			}
			t := time.Now().Add(time.Duration(seconds) * time.Second)
			expiry = &t
		}

		return s.Set(key, value, expiry)

	case "DEL":
		if len(cmd) < 2 {
			return fmt.Errorf("invalid DEL command")
		}
		_, err := s.Del(cmd[1:]...)
		return err

	case "HSET":
		if len(cmd) != 4 {
			return fmt.Errorf("invalid HSET command")
		}
		_, err := s.HSet(cmd[1], cmd[2], cmd[3])
		return err

	case "HDEL":
		if len(cmd) < 3 {
			return fmt.Errorf("invalid HDEL command")
		}
		_, err := s.HDel(cmd[1], cmd[2:]...)
		return err

	case "INCR":
		if len(cmd) != 2 {
			return fmt.Errorf("invalid INCR command")
		}
		_, err := s.Incr(cmd[1])
		return err

	default:
		return fmt.Errorf("unsupported command: %s", command)
	}
}

// Helper function to parse integers
func parseInt(s string) (int, error) {
	var i int
	if _, err := fmt.Sscanf(s, "%d", &i); err != nil {
		return 0, err
	}
	return i, nil
}
