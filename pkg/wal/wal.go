package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"halo-db/pkg/constants"
	"halo-db/pkg/types"
	"os"
	"path/filepath"
	"sync"
)

const (
	OpInsert = "INSERT"
	OpDelete = "DELETE"
)

type LogEntry struct {
	Operation string      `json:"op"`
	Key       types.Key   `json:"key"`
	Value     types.Value `json:"value,omitempty"`
	Timestamp int64       `json:"timestamp"`
}

type WAL interface {
	LogInsert(key types.Key, value types.Value) error
	LogDelete(key types.Key) error
	Replay(insertHandler func(types.Key, types.Value) error, deleteHandler func(types.Key) error) error
	Close() error
	Clear() error
}

type wal struct {
	filePath string
	file     *os.File
	mu       sync.Mutex
}

func NewWAL(dataDir string) (WAL, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	filePath := filepath.Join(dataDir, constants.WALFileName)

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &wal{
		filePath: filePath,
		file:     file,
	}, nil
}

func (w *wal) LogInsert(key types.Key, value types.Value) error {
	entry := LogEntry{
		Operation: OpInsert,
		Key:       key,
		Value:     value,
		Timestamp: getCurrentTimestamp(),
	}
	return w.logEntry(entry)
}

func (w *wal) LogDelete(key types.Key) error {
	entry := LogEntry{
		Operation: OpDelete,
		Key:       key,
		Timestamp: getCurrentTimestamp(),
	}
	return w.logEntry(entry)
}

func (w *wal) logEntry(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}

	length := uint32(len(data))
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, length)

	if _, err := w.file.Write(lengthBytes); err != nil {
		return fmt.Errorf("failed to write length to WAL: %w", err)
	}
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("failed to write data to WAL: %w", err)
	}

	return w.file.Sync()
}

func (w *wal) Replay(insertHandler func(types.Key, types.Value) error, deleteHandler func(types.Key) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		_ = w.file.Close()
	}

	file, err := os.OpenFile(w.filePath, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open WAL file for replay: %w", err)
	}
	defer func() { _ = file.Close() }()

	w.file, err = os.OpenFile(w.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL file for appending: %w", err)
	}

	for {
		lengthBytes := make([]byte, 4)
		if _, err := file.Read(lengthBytes); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to read length from WAL: %w", err)
		}

		length := binary.BigEndian.Uint32(lengthBytes)

		data := make([]byte, length)
		if _, err := file.Read(data); err != nil {
			return fmt.Errorf("failed to read data from WAL: %w", err)
		}

		var entry LogEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			break
		}

		switch entry.Operation {
		case OpInsert:
			if err := insertHandler(entry.Key, entry.Value); err != nil {
				return fmt.Errorf("failed to replay insert operation: %w", err)
			}
		case OpDelete:
			if err := deleteHandler(entry.Key); err != nil {
				return fmt.Errorf("failed to replay delete operation: %w", err)
			}
		default:
			break
		}
	}

	return nil
}

func (w *wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		_ = w.file.Close()
	}
	return nil
}

func (w *wal) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}

	if err := os.Remove(w.filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove WAL file: %w", err)
	}

	file, err := os.OpenFile(w.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new WAL file: %w", err)
	}

	w.file = file
	return nil
}

func getCurrentTimestamp() int64 {
	return int64(0)
}
