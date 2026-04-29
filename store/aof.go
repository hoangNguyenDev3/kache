// Package store implements a sharded concurrent in-memory key-value store.
//
// AOF format:
// The Append-Only File is a sequence of RESP arrays, one per mutating command.
// Each array contains the command name and its arguments as bulk strings.
// On startup the AOF is replayed by parsing each RESP array and invoking the
// corresponding store method, reconstructing the dataset.
package store

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hoangNguyenDev3/kache/resp"
)

// FsyncPolicy controls the durability/performance trade-off for AOF writes.
type FsyncPolicy int

const (
	// FsyncAlways fsyncs after every write. Highest durability, lowest throughput.
	FsyncAlways FsyncPolicy = iota
	// FsyncEverySec fsyncs once per second via a background goroutine.
	FsyncEverySec
	// FsyncNo lets the operating system handle flushing. Highest throughput.
	FsyncNo
)

func (p FsyncPolicy) String() string {
	switch p {
	case FsyncAlways:
		return "always"
	case FsyncEverySec:
		return "everysec"
	case FsyncNo:
		return "no"
	default:
		return "unknown"
	}
}

// AOFWriter handles append-only file operations. It is safe for concurrent use.
type AOFWriter struct {
	file        *os.File
	writer      *bufio.Writer
	done        chan struct{}
	filename    string
	mu          sync.Mutex
	fsyncPolicy FsyncPolicy
}

// NewAOFWriter creates a new AOF writer for the given file with the specified
// fsync policy. If policy is FsyncEverySec, a background goroutine is started
// to flush the file once per second.
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
			if err := w.writer.Flush(); err != nil {
				slog.Warn("aof background fsync: failed to flush", "error", err)
			}
			if err := w.file.Sync(); err != nil {
				slog.Warn("aof background fsync: failed to sync", "error", err)
			}
			w.mu.Unlock()
		}
	}
}

// LogOperation appends a RESP-encoded command to the AOF. The behavior depends
// on the configured fsync policy: FsyncAlways performs an immediate fsync,
// FsyncEverySec relies on the background goroutine, and FsyncNo leaves flushing
// to the OS. LogOperation is safe for concurrent use.
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

// Close flushes any buffered data, stops the background fsync goroutine,
// and closes the underlying file.
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

// Filename returns the path to the AOF file.
func (w *AOFWriter) Filename() string {
	return w.filename
}

// FsyncPolicy returns the current durability policy of the writer.
func (w *AOFWriter) FsyncPolicy() FsyncPolicy {
	return w.fsyncPolicy
}

// LoadAOF replays operations from the AOF file to reconstruct the store state.
// It parses RESP arrays from the file and invokes the corresponding store
// methods (SET, DEL, HSET, LPUSH, etc.). If the file does not exist, LoadAOF
// returns nil without error.
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
	count := 0
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

		count++

		replay := func(cmd string, fn func() error) {
			if err := fn(); err != nil {
				slog.Warn("aof replay: failed to execute command", "command", cmd, "error", err)
			}
		}

		switch args[0] {
		case "SET":
			if len(args) >= 5 && args[3] == "EX" {
				seconds, _ := strconv.Atoi(args[4])
				expiry := time.Now().Add(time.Duration(seconds) * time.Second)
				replay("SET", func() error { return s.Set(args[1], args[2], &expiry) })
			} else if len(args) >= 3 {
				replay("SET", func() error { return s.Set(args[1], args[2], nil) })
			}
		case "DEL":
			if len(args) >= 2 {
				replay("DEL", func() error { _, err := s.Del(args[1:]...); return err })
			}
		case "HSET":
			if len(args) >= 4 {
				replay("HSET", func() error { _, err := s.HSet(args[1], args[2], args[3]); return err })
			}
		case "HDEL":
			if len(args) >= 3 {
				replay("HDEL", func() error { _, err := s.HDel(args[1], args[2:]...); return err })
			}
		case "INCR":
			if len(args) >= 2 {
				replay("INCR", func() error { _, err := s.Incr(args[1]); return err })
			}
		case "LPUSH":
			if len(args) >= 3 {
				replay("LPUSH", func() error { _, err := s.LPush(args[1], args[2:]...); return err })
			}
		case "RPUSH":
			if len(args) >= 3 {
				replay("RPUSH", func() error { _, err := s.RPush(args[1], args[2:]...); return err })
			}
		case "LPOP":
			if len(args) >= 2 {
				replay("LPOP", func() error { _, err := s.LPop(args[1]); return err })
			}
		case "RPOP":
			if len(args) >= 2 {
				replay("RPOP", func() error { _, err := s.RPop(args[1]); return err })
			}
		case "LTRIM":
			if len(args) >= 4 {
				start, _ := strconv.Atoi(args[2])
				stop, _ := strconv.Atoi(args[3])
				replay("LTRIM", func() error { return s.LTrim(args[1], start, stop) })
			}
		}
	}

	slog.Info("aof file loaded", "commands", count, "path", filename, "component", "aof")
	return nil
}

// EnableAOF creates a new AOFWriter and attaches it to the store.
// Subsequent mutating operations will be logged to the AOF.
func (s *Store) EnableAOF(filename string, policy FsyncPolicy) error {
	writer, err := NewAOFWriter(filename, policy)
	if err != nil {
		return err
	}
	s.aof = writer
	slog.Info("aof persistence enabled", "path", filename, "fsync_policy", policy.String(), "component", "aof")
	return nil
}

// DisableAOF closes the current AOF writer and detaches it from the store.
func (s *Store) DisableAOF() error {
	if s.aof != nil {
		if err := s.aof.Close(); err != nil {
			return err
		}
		s.aof = nil
	}
	return nil
}
