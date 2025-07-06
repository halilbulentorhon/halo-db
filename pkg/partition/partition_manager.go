package partition

import (
	"crypto/md5"
	"encoding/binary"
	"halo-db/pkg/btree"
	"halo-db/pkg/types"
	"sync"
)

type PartitionManager struct {
	partitions []*Partition
	numParts   int
	mu         sync.RWMutex
}

func NewPartitionManager(numPartitions int, dataDir string) (*PartitionManager, error) {
	pm := &PartitionManager{
		partitions: make([]*Partition, numPartitions),
		numParts:   numPartitions,
	}

	for i := 0; i < numPartitions; i++ {
		partition, err := NewPartition(i, dataDir)
		if err != nil {
			return nil, err
		}
		pm.partitions[i] = partition
	}

	if err := pm.replayWALs(); err != nil {
		return nil, err
	}

	return pm, nil
}

func (pm *PartitionManager) GetPartition(key types.Key) *Partition {
	hash := hashKey(key)
	partitionID := hash % uint32(pm.numParts)
	return pm.partitions[partitionID]
}

func (pm *PartitionManager) Put(key types.Key, value types.Value) error {
	partition := pm.GetPartition(key)
	partition.mu.Lock()
	defer partition.mu.Unlock()

	if err := partition.WAL.LogInsert(key, value); err != nil {
		return err
	}

	partition.Memtable.Put(key, value)

	if partition.Memtable.IsFull() {
		if err := partition.flushMemtable(); err != nil {
			return err
		}
	}

	return nil
}

func (pm *PartitionManager) Get(key types.Key) (types.Value, error) {
	partition := pm.GetPartition(key)
	partition.mu.RLock()
	defer partition.mu.RUnlock()

	if value, found := partition.Memtable.Get(key); found {
		if value == nil {
			return nil, btree.ErrKeyNotFound
		}
		return value, nil
	}

	return partition.Tree.Find(key)
}

func (pm *PartitionManager) Delete(key types.Key) error {
	partition := pm.GetPartition(key)
	partition.mu.Lock()
	defer partition.mu.Unlock()

	if err := partition.WAL.LogDelete(key); err != nil {
		return err
	}

	partition.Memtable.Delete(key)

	if partition.Memtable.IsFull() {
		if err := partition.flushMemtable(); err != nil {
			return err
		}
	}

	return nil
}

func (pm *PartitionManager) List() []types.Key {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	allKeys := make(map[types.Key]bool)

	for _, partition := range pm.partitions {
		partition.mu.RLock()

		if partition.Tree.Root != nil {
			for _, key := range partition.collectAllKeys(partition.Tree.Root) {
				allKeys[key] = true
			}
		}

		for _, entry := range partition.Memtable.GetAllEntries() {
			allKeys[entry.Key] = true
		}

		partition.mu.RUnlock()
	}

	result := make([]types.Key, 0, len(allKeys))
	for key := range allKeys {
		result = append(result, key)
	}

	return result
}

func (pm *PartitionManager) Clear() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, partition := range pm.partitions {
		partition.mu.Lock()
		if err := partition.WAL.Clear(); err != nil {
			partition.mu.Unlock()
			return err
		}
		partition.Tree = btree.NewBPlusTree()
		partition.Memtable.Clear()
		partition.mu.Unlock()
	}

	return nil
}

func (pm *PartitionManager) Close() error {
	for _, partition := range pm.partitions {
		if err := partition.WAL.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (pm *PartitionManager) GetStats() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	totalKeys := 0
	totalMemtableSize := 0

	for _, partition := range pm.partitions {
		partition.mu.RLock()
		totalKeys += len(partition.List())
		totalMemtableSize += partition.Memtable.GetSize()
		partition.mu.RUnlock()
	}

	return map[string]interface{}{
		"total_keys":          totalKeys,
		"total_memtable_size": totalMemtableSize,
		"num_partitions":      pm.numParts,
		"bloom_filter":        "enabled",
		"partitioning":        "enabled",
	}
}

func (pm *PartitionManager) replayWALs() error {
	for _, partition := range pm.partitions {
		if err := partition.replayWAL(); err != nil {
			return err
		}
	}
	return nil
}

func hashKey(key types.Key) uint32 {
	hash := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(hash[:4])
}
