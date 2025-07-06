package partition

import (
	"crypto/md5"
	"encoding/binary"
	"halo-db/pkg/types"
	"sync"
)

type PartitionManager interface {
	Put(key types.Key, value types.Value) error
	Get(key types.Key) (types.Value, error)
	Delete(key types.Key) error
	List() []types.Key
	Clear() error
	Close() error
	GetStats() map[string]interface{}
	GetPartition(key types.Key) Partition
}

type partitionManager struct {
	partitions []Partition
	numParts   int
	mu         sync.RWMutex
}

func NewPartitionManager(numPartitions int, dataDir string) (PartitionManager, error) {
	pm := &partitionManager{
		partitions: make([]Partition, numPartitions),
		numParts:   numPartitions,
	}

	for i := 0; i < numPartitions; i++ {
		pt, err := NewPartition(i, dataDir)
		if err != nil {
			return nil, err
		}
		pm.partitions[i] = pt
	}

	return pm, nil
}

func (pm *partitionManager) GetPartition(key types.Key) Partition {
	hash := hashKey(key)
	partitionID := hash % uint32(pm.numParts)
	return pm.partitions[partitionID]
}

func (pm *partitionManager) Put(key types.Key, value types.Value) error {
	pt := pm.GetPartition(key)
	return pt.Put(key, value)
}

func (pm *partitionManager) Get(key types.Key) (types.Value, error) {
	pt := pm.GetPartition(key)
	return pt.Get(key)
}

func (pm *partitionManager) Delete(key types.Key) error {
	pt := pm.GetPartition(key)
	return pt.Delete(key)
}

func (pm *partitionManager) List() []types.Key {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	allKeys := make(map[types.Key]bool)

	for _, pt := range pm.partitions {
		for _, key := range pt.List() {
			allKeys[key] = true
		}
	}

	result := make([]types.Key, 0, len(allKeys))
	for key := range allKeys {
		result = append(result, key)
	}

	return result
}

func (pm *partitionManager) Clear() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, pt := range pm.partitions {
		if err := pt.Clear(); err != nil {
			return err
		}
	}

	return nil
}

func (pm *partitionManager) Close() error {
	for _, pt := range pm.partitions {
		if err := pt.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (pm *partitionManager) GetStats() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	totalKeys := 0

	for _, pt := range pm.partitions {
		totalKeys += len(pt.List())
	}

	return map[string]interface{}{
		"total_keys":     totalKeys,
		"num_partitions": pm.numParts,
		"bloom_filter":   "enabled",
		"partitioning":   "enabled",
	}
}

func hashKey(key types.Key) uint32 {
	hash := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(hash[:4])
}
