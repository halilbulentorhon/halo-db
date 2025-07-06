package store

import (
	"errors"
	"fmt"
	"halo-db/pkg/btree"
	"halo-db/pkg/memtable"
	"halo-db/pkg/types"
	"halo-db/pkg/wal"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Store struct {
	tree     *btree.BPlusTree
	memtable *memtable.Memtable
	wal      *wal.WAL
	dataDir  string
	mu       sync.RWMutex
	stopChan chan struct{}
}

func NewStore(dataDir string) (*Store, error) {
	tree := btree.NewBPlusTree()
	mTable := memtable.NewMemtable(1000)

	w, err := wal.NewWAL(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	store := &Store{
		tree:     tree,
		memtable: mTable,
		wal:      w,
		dataDir:  dataDir,
		stopChan: make(chan struct{}),
	}

	if err := store.replayWAL(); err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	go store.backgroundFlush()

	return store, nil
}

func (s *Store) Put(key types.Key, value types.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.LogInsert(key, value); err != nil {
		return fmt.Errorf("failed to log insert to WAL: %w", err)
	}

	s.memtable.Put(key, value)

	if s.memtable.IsFull() {
		if err := s.flushMemtable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	}

	return nil
}

func (s *Store) Get(key types.Key) (types.Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if value, found := s.memtable.Get(key); found {
		if value == nil {
			return nil, fmt.Errorf("key not found")
		}
		return value, nil
	}

	return s.tree.Find(key)
}

func (s *Store) Delete(key types.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.LogDelete(key); err != nil {
		return fmt.Errorf("failed to log delete to WAL: %w", err)
	}

	s.memtable.Delete(key)

	if s.memtable.IsFull() {
		if err := s.flushMemtable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	}

	return nil
}

func (s *Store) List() []types.Key {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make(map[types.Key]bool)

	if s.tree.Root != nil {
		for _, key := range s.collectAllKeys(s.tree.Root) {
			keys[key] = true
		}
	}

	for _, entry := range s.memtable.GetAllEntries() {
		if entry.Value != nil {
			keys[entry.Key] = true
		} else {
			delete(keys, entry.Key)
		}
	}

	result := make([]types.Key, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}

	return result
}

func (s *Store) GetTree() *btree.BPlusTree {
	return s.tree
}

func (s *Store) Close() error {
	close(s.stopChan)
	return s.wal.Close()
}

func (s *Store) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.Clear(); err != nil {
		return fmt.Errorf("failed to clear WAL: %w", err)
	}

	s.tree = btree.NewBPlusTree()
	s.memtable.Clear()

	return nil
}

func (s *Store) flushMemtable() error {
	entries := s.memtable.GetAllEntries()

	for _, entry := range entries {
		if entry.Value != nil {
			if err := s.tree.Insert(entry.Key, entry.Value); err != nil {
				return fmt.Errorf("failed to insert into B+ tree: %w", err)
			}
		} else {
			if err := s.tree.Delete(entry.Key); err != nil {
				return fmt.Errorf("failed to delete from B+ tree: %w", err)
			}
		}
	}

	s.memtable.Clear()
	return nil
}

func (s *Store) backgroundFlush() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if s.memtable.GetSize() > 0 {
				err := s.flushMemtable()
				if err != nil {
					panic("error flushing memtable")
				}
			}
			s.mu.Unlock()
		case <-s.stopChan:
			return
		}
	}
}

func (s *Store) replayWAL() error {
	s.tree.SkipBloomFilter(true)
	defer s.tree.SkipBloomFilter(false)

	insertHandler := func(key types.Key, value types.Value) error {
		return s.tree.Insert(key, value)
	}

	deleteHandler := func(key types.Key) error {
		err := s.tree.Delete(key)
		if err != nil && (errors.Is(err, btree.ErrKeyNotFound) || strings.Contains(err.Error(), "key not found")) {
			return nil
		}
		return err
	}

	return s.wal.Replay(insertHandler, deleteHandler)
}

func (s *Store) collectAllKeys(node *btree.Node) []types.Key {
	var keys []types.Key

	if node.IsLeaf {
		keys = append(keys, node.Keys...)
	} else {
		for _, child := range node.Children {
			keys = append(keys, s.collectAllKeys(child)...)
		}
	}

	return keys
}

func (s *Store) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := s.List()

	walPath := filepath.Join(s.dataDir, "wal.log")
	fileInfo, err := os.Stat(walPath)
	walSize := int64(0)
	if err == nil {
		walSize = fileInfo.Size()
	}

	return map[string]interface{}{
		"total_keys":    len(keys),
		"memtable_size": s.memtable.GetSize(),
		"wal_size":      walSize,
		"data_dir":      s.dataDir,
		"bloom_filter":  "enabled",
	}
}
