package store

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hoangNguyenDev3/kache/resp"
)

type FsyncPolicy int

const (
	FsyncAlways   FsyncPolicy = iota // fsync after every write
	FsyncEverySec                    // fsync once per second via background goroutine
	FsyncNo                          // let OS handle it
)

// AOFWriter handles append-only file operations
type AOFWriter struct {
	mu          sync.Mutex
	file        *os.File
	writer      *bufio.Writer
	filename    string
	fsyncPolicy FsyncPolicy
	done        chan struct{}
}

// NewAOFWriter creates a new AOF writer
func NewAOFWriter(filename string, policy FsyncPolicy) (*AOFWriter, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open AOF file: %w", err)
	}

	w := &AOFWriter{
		file:        file,
		writer:      bufio.NewWriter(file),
		filename:    filename,
		fsyncPolicy: policy,
		done:        make(chan struct{}),
	}

	if policy == FsyncEverySec {
		go w.backgroundFsync()
	}

	return w, nil
}

func (w *AOFWriter) backgroundFsync() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			w.mu.Lock()
			w.writer.Flush()
			w.file.Sync()
			w.mu.Unlock()
		}
	}
}

// LogOperation writes an operation to the AOF in RESP format
func (w *AOFWriter) LogOperation(cmd []string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := resp.FormatCommand(cmd)
	if _, err := w.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write operation: %w", err)
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush operation: %w", err)
	}

	if w.fsyncPolicy == FsyncAlways {
		return w.file.Sync()
	}

	return nil
}

// Close closes the AOF writer and stops the background fsync goroutine
func (w *AOFWriter) Close() error {
	if w.done != nil {
		close(w.done)
		w.done = nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Filename returns the AOF filename
func (w *AOFWriter) Filename() string {
	return w.filename
}

// FsyncPolicy returns the fsync policy
func (w *AOFWriter) FsyncPolicy() FsyncPolicy {
	return w.fsyncPolicy
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

	reader := bufio.NewReader(file)
	for {
		value, err := resp.Parse(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to parse AOF entry: %w", err)
		}

		if value.Type != resp.Array {
			continue
		}

		args := make([]string, len(value.Array))
		for i, v := range value.Array {
			args[i] = v.Str
		}

		if len(args) == 0 {
			continue
		}

		switch args[0] {
		case "SET":
			if len(args) >= 5 && args[3] == "EX" {
				seconds, _ := strconv.Atoi(args[4])
				expiry := time.Now().Add(time.Duration(seconds) * time.Second)
				s.Set(args[1], args[2], &expiry)
			} else if len(args) >= 3 {
				s.Set(args[1], args[2], nil)
			}
		case "DEL":
			if len(args) >= 2 {
				s.Del(args[1:]...)
			}
		case "HSET":
			if len(args) >= 4 {
				s.HSet(args[1], args[2], args[3])
			}
		case "HDEL":
			if len(args) >= 3 {
				s.HDel(args[1], args[2:]...)
			}
		case "INCR":
			if len(args) >= 2 {
				s.Incr(args[1])
			}
		case "LPUSH":
			if len(args) >= 3 {
				s.LPush(args[1], args[2:]...)
			}
		case "RPUSH":
			if len(args) >= 3 {
				s.RPush(args[1], args[2:]...)
			}
		case "LPOP":
			if len(args) >= 2 {
				s.LPop(args[1])
			}
		case "RPOP":
			if len(args) >= 2 {
				s.RPop(args[1])
			}
		case "LTRIM":
			if len(args) >= 4 {
				start, _ := strconv.Atoi(args[2])
				stop, _ := strconv.Atoi(args[3])
				s.LTrim(args[1], start, stop)
			}
		}
	}

	return nil
}

// EnableAOF enables AOF persistence for the store
func (s *Store) EnableAOF(filename string, policy FsyncPolicy) error {
	writer, err := NewAOFWriter(filename, policy)
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
