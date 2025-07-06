package wal

import (
	"halo-db/pkg/types"
	"os"
	"path/filepath"
	"testing"
)

func TestWALBasicOperations(t *testing.T) {

	tempDir := t.TempDir()

	wal, err := NewWAL(tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	testKey := types.Key("test-key")
	testValue := types.Value("test-value")

	err = wal.LogInsert(testKey, testValue)
	if err != nil {
		t.Fatalf("Failed to log insert: %v", err)
	}

	err = wal.LogDelete(testKey)
	if err != nil {
		t.Fatalf("Failed to log delete: %v", err)
	}
}

func TestWALReplay(t *testing.T) {

	tempDir := t.TempDir()

	wal, err := NewWAL(tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	operations := []struct {
		op    string
		key   types.Key
		value types.Value
	}{
		{"insert", "key1", []byte("value1")},
		{"insert", "key2", []byte("value2")},
		{"delete", "key1", nil},
		{"insert", "key3", []byte("value3")},
	}

	for _, op := range operations {
		if op.op == "insert" {
			err = wal.LogInsert(op.key, op.value)
		} else {
			err = wal.LogDelete(op.key)
		}
		if err != nil {
			t.Fatalf("Failed to log operation: %v", err)
		}
	}

	_ = wal.Close()

	wal2, err := NewWAL(tempDir)
	if err != nil {
		t.Fatalf("Failed to create second WAL: %v", err)
	}
	defer func() { _ = wal2.Close() }()

	var replayedOps []struct {
		op    string
		key   types.Key
		value types.Value
	}

	insertHandler := func(key types.Key, value types.Value) error {
		replayedOps = append(replayedOps, struct {
			op    string
			key   types.Key
			value types.Value
		}{"insert", key, value})
		return nil
	}

	deleteHandler := func(key types.Key) error {
		replayedOps = append(replayedOps, struct {
			op    string
			key   types.Key
			value types.Value
		}{"delete", key, nil})
		return nil
	}

	err = wal2.Replay(insertHandler, deleteHandler)
	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if len(replayedOps) != len(operations) {
		t.Fatalf("Expected %d replayed operations, got %d", len(operations), len(replayedOps))
	}

	for i, op := range operations {
		if replayedOps[i].op != op.op || replayedOps[i].key != op.key {
			t.Errorf("Operation %d mismatch: expected %+v, got %+v", i, op, replayedOps[i])
		}

		if op.value != nil && string(replayedOps[i].value) != string(op.value) {
			t.Errorf("Operation %d value mismatch: expected %s, got %s", i, string(op.value), string(replayedOps[i].value))
		}
	}
}

func TestWALClear(t *testing.T) {

	tempDir := t.TempDir()

	wal, err := NewWAL(tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	err = wal.LogInsert("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to log insert: %v", err)
	}

	err = wal.Clear()
	if err != nil {
		t.Fatalf("Failed to clear WAL: %v", err)
	}

	walPath := filepath.Join(tempDir, "wal.log")
	fileInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if fileInfo.Size() != 0 {
		t.Errorf("Expected WAL file to be empty after clear, got size %d", fileInfo.Size())
	}
}

func TestWALNewDatabase(t *testing.T) {

	tempDir := t.TempDir()

	wal, err := NewWAL(tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	insertCount := 0
	deleteCount := 0

	insertHandler := func(key types.Key, value types.Value) error {
		insertCount++
		return nil
	}

	deleteHandler := func(key types.Key) error {
		deleteCount++
		return nil
	}

	err = wal.Replay(insertHandler, deleteHandler)
	if err != nil {
		t.Fatalf("Failed to replay empty WAL: %v", err)
	}

	if insertCount != 0 || deleteCount != 0 {
		t.Errorf("Expected no operations to be replayed, got %d inserts and %d deletes", insertCount, deleteCount)
	}
}
