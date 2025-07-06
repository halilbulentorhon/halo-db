package partition

import (
	"fmt"
	"halo-db/pkg/types"
	"os"
	"sync"
	"testing"
)

func TestPartitionManager(t *testing.T) {
	dataDir := "test_data_partition"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}
	defer func() { _ = pm.Close() }()

	testKey := types.Key("test_key")
	testValue := types.Value("test_value")

	if err := pm.Put(testKey, testValue); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	value, err := pm.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if string(value) != string(testValue) {
		t.Fatalf("Expected %s, got %s", testValue, value)
	}

	if err := pm.Delete(testKey); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	_, err = pm.Get(testKey)
	if err == nil {
		t.Fatalf("Expected error after delete")
	}
}

func TestPartitionDistribution(t *testing.T) {
	dataDir := "test_data_distribution"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}
	defer func() { _ = pm.Close() }()

	partitionMap := make(map[int][]types.Key)

	keys := []types.Key{"key1", "key2", "key3", "key4", "key5", "user:123", "user:456", "user:789", "session:abc", "session:def"}

	for _, key := range keys {
		partition := pm.GetPartition(key)
		partitionMap[partition.GetID()] = append(partitionMap[partition.GetID()], key)
	}

	t.Logf("Partition distribution: %v", partitionMap)

	hasMultiplePartitions := false
	for _, keys := range partitionMap {
		if len(keys) > 0 {
			hasMultiplePartitions = true
			break
		}
	}

	if !hasMultiplePartitions {
		t.Fatal("All keys went to the same partition")
	}

	for _, key := range keys {
		expected := types.Value("value_" + string(key))
		if err := pm.Put(key, expected); err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	for _, key := range keys {
		value, err := pm.Get(key)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		expected := types.Value("value_" + string(key))
		if string(value) != string(expected) {
			t.Fatalf("Expected %s, got %s for key %s", expected, value, key)
		}
	}
}

func TestPartitionList(t *testing.T) {
	dataDir := "test_data_list"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(3, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}
	defer func() { _ = pm.Close() }()

	keys := []types.Key{"key1", "key2", "key3", "key4", "key5"}

	for _, key := range keys {
		if err := pm.Put(key, types.Value("value_"+string(key))); err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	allKeys := pm.List()

	if len(allKeys) != len(keys) {
		t.Fatalf("Expected %d keys, got %d", len(keys), len(allKeys))
	}

	keyMap := make(map[types.Key]bool)
	for _, key := range allKeys {
		keyMap[key] = true
	}

	for _, expectedKey := range keys {
		if !keyMap[expectedKey] {
			t.Fatalf("Expected key %s not found in list", expectedKey)
		}
	}
}

func TestPartitionClear(t *testing.T) {
	dataDir := "test_data_clear"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(3, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}
	defer func() { _ = pm.Close() }()

	keys := []types.Key{"key1", "key2", "key3"}

	for _, key := range keys {
		if err := pm.Put(key, types.Value("value_"+string(key))); err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	for _, key := range keys {
		value, err := pm.Get(key)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		expected := types.Value("value_" + string(key))
		if string(value) != string(expected) {
			t.Fatalf("Expected %s, got %s for key %s", expected, value, key)
		}
	}

	if err := pm.Clear(); err != nil {
		t.Fatalf("Failed to clear: %v", err)
	}

	allKeys := pm.List()
	if len(allKeys) != 0 {
		t.Fatalf("Expected 0 keys after clear, got %d", len(allKeys))
	}
}

func TestPartitionStats(t *testing.T) {
	dataDir := "test_data_stats"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(3, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}
	defer func() { _ = pm.Close() }()

	keys := []types.Key{"key1", "key2", "key3", "key4", "key5"}

	for _, key := range keys {
		if err := pm.Put(key, types.Value("value_"+string(key))); err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	stats := pm.GetStats()
	t.Logf("Stats: %v", stats)

	if stats["total_keys"] != 5 {
		t.Fatalf("Expected 5 total keys, got %v", stats["total_keys"])
	}

	if stats["num_partitions"] != 3 {
		t.Fatalf("Expected 3 partitions, got %v", stats["num_partitions"])
	}
}

func TestPartitionConcurrency(t *testing.T) {
	dataDir := "test_data_concurrency"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}
	defer func() { _ = pm.Close() }()

	const numGoroutines = 10
	const keysPerGoroutine = 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*keysPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < keysPerGoroutine; j++ {
				key := types.Key(fmt.Sprintf("goroutine_%d_key_%d", id, j))
				value := types.Value(fmt.Sprintf("value_%d_%d", id, j))
				if err := pm.Put(key, value); err != nil {
					errors <- fmt.Errorf("goroutine %d failed to put key %s: %v", id, key, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent operation error: %v", err)
	}

	allKeys := pm.List()
	expectedKeys := numGoroutines * keysPerGoroutine
	if len(allKeys) != expectedKeys {
		t.Fatalf("Expected %d keys after concurrent operations, got %d", expectedKeys, len(allKeys))
	}
}
