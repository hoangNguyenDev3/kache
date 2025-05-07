package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

const rdbMagic = "KACHE"
const rdbVersion = 1

type rdbEntry struct {
	key   string
	entry *Entry
}

// SaveRDB saves the current state to an RDB file using a custom binary format
func (s *Store) SaveRDB(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create RDB file: %w", err)
	}
	defer file.Close()

	// Write header: magic bytes + version byte
	if _, err := file.Write([]byte(rdbMagic)); err != nil {
		return fmt.Errorf("failed to write RDB header: %w", err)
	}
	if _, err := file.Write([]byte{rdbVersion}); err != nil {
		return fmt.Errorf("failed to write RDB version: %w", err)
	}

	// Progressive shard-by-shard snapshot
	var allEntries []rdbEntry
	for i := 0; i < NumShards; i++ {
		shard := s.shards[i]
		shard.mu.RLock()
		entries := make([]rdbEntry, 0, len(shard.data))
		for key, entry := range shard.data {
			if isExpired(entry.ExpiresAt) {
				continue
			}
			entries = append(entries, rdbEntry{key: key, entry: entry})
		}
		shard.mu.RUnlock()
		allEntries = append(allEntries, entries...)
	}

	// Encode all entries
	for _, e := range allEntries {
		if err := writeEntry(file, e.key, e.entry); err != nil {
			return fmt.Errorf("failed to write RDB entry: %w", err)
		}
	}

	return nil
}

func writeEntry(file *os.File, key string, entry *Entry) error {
	// Write key
	if err := binary.Write(file, binary.BigEndian, uint32(len(key))); err != nil {
		return err
	}
	if _, err := file.WriteString(key); err != nil {
		return err
	}

	// Determine type and serialize value
	var typeByte byte
	var valueBytes []byte
	var err error

	switch v := entry.Value.(type) {
	case string:
		typeByte = 0
		valueBytes = []byte(v)
	case int64:
		typeByte = 1
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		valueBytes = buf
	case *Hash:
		typeByte = 2
		valueBytes, err = serializeHash(v)
		if err != nil {
			return err
		}
	case *List:
		typeByte = 3
		elems := v.GetElements()
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, uint32(len(elems)))
		for _, e := range elems {
			eBytes := []byte(e)
			binary.Write(buf, binary.BigEndian, uint32(len(eBytes)))
			buf.Write(eBytes)
		}
		valueBytes = buf.Bytes()
	default:
		// Skip unknown types
		return nil
	}

	// Write type byte
	if _, err := file.Write([]byte{typeByte}); err != nil {
		return err
	}

	// Write value
	if err := binary.Write(file, binary.BigEndian, uint32(len(valueBytes))); err != nil {
		return err
	}
	if _, err := file.Write(valueBytes); err != nil {
		return err
	}

	// Write expiry
	if entry.ExpiresAt != nil {
		if _, err := file.Write([]byte{1}); err != nil {
			return err
		}
		if err := binary.Write(file, binary.BigEndian, entry.ExpiresAt.UnixNano()); err != nil {
			return err
		}
	} else {
		if _, err := file.Write([]byte{0}); err != nil {
			return err
		}
	}

	return nil
}

func serializeHash(h *Hash) ([]byte, error) {
	fields := h.GetFields()
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, uint32(len(fields))); err != nil {
		return nil, err
	}
	for k, v := range fields {
		if err := binary.Write(buf, binary.BigEndian, uint32(len(k))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(k); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, uint32(len(v))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(v); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// LoadRDB loads the state from an RDB file
func (s *Store) LoadRDB(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	// Read header
	header := make([]byte, 6)
	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("failed to read RDB header: %w", err)
	}
	if string(header[:5]) != rdbMagic {
		return fmt.Errorf("invalid RDB magic: %s", string(header[:5]))
	}
	if header[5] != rdbVersion {
		return fmt.Errorf("unsupported RDB version: %d", header[5])
	}

	// Clear current data in all shards
	for _, shard := range s.shards {
		shard.mu.Lock()
		for key := range shard.data {
			delete(shard.data, key)
		}
		shard.mu.Unlock()
	}

	// Read entries until EOF
	for {
		key, entry, err := readEntry(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read RDB entry: %w", err)
		}
		if entry == nil {
			break
		}

		if isExpired(entry.ExpiresAt) {
			continue
		}

		shard := s.getShard(key)
		shard.mu.Lock()
		shard.data[key] = entry
		shard.mu.Unlock()
	}

	return nil
}

func readEntry(file *os.File) (string, *Entry, error) {
	// Read key length
	var keyLen uint32
	if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
		if errors.Is(err, io.EOF) {
			return "", nil, err
		}
		return "", nil, err
	}

	// Read key
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(file, key); err != nil {
		return "", nil, err
	}

	// Read type byte
	typeByte := make([]byte, 1)
	if _, err := io.ReadFull(file, typeByte); err != nil {
		return "", nil, err
	}

	// Read value length
	var valueLen uint32
	if err := binary.Read(file, binary.BigEndian, &valueLen); err != nil {
		return "", nil, err
	}

	// Read value
	valueBytes := make([]byte, valueLen)
	if _, err := io.ReadFull(file, valueBytes); err != nil {
		return "", nil, err
	}

	var value interface{}
	switch typeByte[0] {
	case 0:
		value = string(valueBytes)
	case 1:
		if len(valueBytes) != 8 {
			return "", nil, fmt.Errorf("invalid int64 value length: %d", len(valueBytes))
		}
		value = int64(binary.BigEndian.Uint64(valueBytes))
	case 2:
		hash, err := deserializeHash(valueBytes)
		if err != nil {
			return "", nil, err
		}
		value = hash
	case 3: // List
		reader := bytes.NewReader(valueBytes)
		var numElems uint32
		if err := binary.Read(reader, binary.BigEndian, &numElems); err != nil {
			return "", nil, fmt.Errorf("failed to read list element count: %w", err)
		}
		list := NewList()
		for i := uint32(0); i < numElems; i++ {
			var elemLen uint32
			if err := binary.Read(reader, binary.BigEndian, &elemLen); err != nil {
				return "", nil, fmt.Errorf("failed to read list element length: %w", err)
			}
			elemBytes := make([]byte, elemLen)
			if _, err := io.ReadFull(reader, elemBytes); err != nil {
				return "", nil, fmt.Errorf("failed to read list element: %w", err)
			}
			list.elements = append(list.elements, string(elemBytes))
		}
		value = list
	default:
		return "", nil, fmt.Errorf("unknown type byte: %d", typeByte[0])
	}

	// Read has_expiry
	hasExpiry := make([]byte, 1)
	if _, err := io.ReadFull(file, hasExpiry); err != nil {
		return "", nil, err
	}

	var expiry *time.Time
	if hasExpiry[0] == 1 {
		var expiryNs int64
		if err := binary.Read(file, binary.BigEndian, &expiryNs); err != nil {
			return "", nil, err
		}
		t := time.Unix(0, expiryNs)
		expiry = &t
	}

	return string(key), &Entry{
		Value:     value,
		ExpiresAt: expiry,
	}, nil
}

func deserializeHash(data []byte) (*Hash, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid hash data")
	}
	buf := bytes.NewReader(data)
	var numFields uint32
	if err := binary.Read(buf, binary.BigEndian, &numFields); err != nil {
		return nil, err
	}

	h := NewHash()
	for i := 0; i < int(numFields); i++ {
		var fkLen uint32
		if err := binary.Read(buf, binary.BigEndian, &fkLen); err != nil {
			return nil, err
		}
		fk := make([]byte, fkLen)
		if _, err := io.ReadFull(buf, fk); err != nil {
			return nil, err
		}
		var fvLen uint32
		if err := binary.Read(buf, binary.BigEndian, &fvLen); err != nil {
			return nil, err
		}
		fv := make([]byte, fvLen)
		if _, err := io.ReadFull(buf, fv); err != nil {
			return nil, err
		}
		h.Fields[string(fk)] = string(fv)
	}
	return h, nil
}
