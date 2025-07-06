package partition

import (
	"fmt"
	"halo-db/pkg/types"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPartitionFuzz(t *testing.T) {
	dataDir := "test_data_fuzz"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}
	defer func() { _ = pm.Close() }()

	expectedState := make(map[types.Key]types.Value)
	var mu sync.RWMutex

	numOperations := 1000

	for i := 0; i < numOperations; i++ {
		operation := rand.Intn(3)
		key := types.Key(fmt.Sprintf("fuzz_key_%d", rand.Intn(100)))
		value := types.Value(fmt.Sprintf("fuzz_value_%d", rand.Intn(1000)))

		switch operation {
		case 0:
			if err := pm.Put(key, value); err != nil {
				t.Errorf("Fuzz put failed: %v", err)
			} else {
				mu.Lock()
				expectedState[key] = value
				mu.Unlock()
			}
		case 1:
			if _, err := pm.Get(key); err != nil {
				mu.RLock()
				if _, exists := expectedState[key]; exists {
					t.Errorf("Fuzz get failed for existing key %s: %v", key, err)
				}
				mu.RUnlock()
			}
		case 2:
			if err := pm.Delete(key); err != nil {
				mu.RLock()
				if _, exists := expectedState[key]; !exists {
					t.Errorf("Fuzz delete failed for non-existing key %s: %v", key, err)
				}
				mu.RUnlock()
			} else {
				mu.Lock()
				delete(expectedState, key)
				mu.Unlock()
			}
		}

		if i%100 == 0 {
			mu.RLock()
			for k, v := range expectedState {
				if actual, err := pm.Get(k); err != nil {
					t.Errorf("Verification failed for key %s: %v", k, err)
				} else if string(actual) != string(v) {
					t.Errorf("Value mismatch for key %s: expected %s, got %s", k, v, actual)
				}
			}
			mu.RUnlock()
		}
	}

	mu.RLock()
	for k, v := range expectedState {
		if actual, err := pm.Get(k); err != nil {
			t.Errorf("Final verification failed for key %s: %v", k, err)
		} else if string(actual) != string(v) {
			t.Errorf("Final value mismatch for key %s: expected %s, got %s", k, v, actual)
		}
	}
	mu.RUnlock()
}

func TestPartitionStress(t *testing.T) {
	dataDir := "test_data_stress"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}
	defer func() { _ = pm.Close() }()

	var mu sync.RWMutex
	operations := make(map[types.Key]types.Value)

	numGoroutines := 50
	numOperationsPerGoroutine := 100
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numOperationsPerGoroutine; j++ {
				key := types.Key(fmt.Sprintf("stress_%d_%d", goroutineID, j))
				value := types.Value(fmt.Sprintf("stress_value_%d_%d", goroutineID, j))

				if err := pm.Put(key, value); err != nil {
					t.Errorf("Stress put failed: %v", err)
					return
				}

				mu.Lock()
				operations[key] = value
				mu.Unlock()

				if actual, err := pm.Get(key); err != nil {
					t.Errorf("Stress get failed: %v", err)
				} else if string(actual) != string(value) {
					t.Errorf("Stress value mismatch: expected %s, got %s", value, actual)
				}

				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	mu.RLock()
	for key, expectedValue := range operations {
		actualValue, err := pm.Get(key)
		if err != nil {
			t.Errorf("Stress verification failed for key %s: %v", key, err)
		} else if string(actualValue) != string(expectedValue) {
			t.Errorf("Stress verification mismatch for key %s: expected %s, got %s", key, expectedValue, actualValue)
		}
	}
	mu.RUnlock()
}

func TestPartitionEdgeCases(t *testing.T) {
	dataDir := "test_data_edge"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}
	defer func() { _ = pm.Close() }()

	emptyKey := types.Key("")
	emptyValue := types.Value("empty_value")
	if err := pm.Put(emptyKey, emptyValue); err != nil {
		t.Errorf("Failed to put empty key: %v", err)
	}

	if value, err := pm.Get(emptyKey); err != nil {
		t.Errorf("Failed to get empty key: %v", err)
	} else if string(value) != string(emptyValue) {
		t.Errorf("Empty key value mismatch")
	}

	longKey := types.Key(string(make([]byte, 1000)))
	longValue := types.Value(string(make([]byte, 10000)))
	if err := pm.Put(longKey, longValue); err != nil {
		t.Errorf("Failed to put long key/value: %v", err)
	}

	if value, err := pm.Get(longKey); err != nil {
		t.Errorf("Failed to get long key: %v", err)
	} else if string(value) != string(longValue) {
		t.Errorf("Long key/value mismatch")
	}

	specialKey := types.Key("key\nwith\ttabs\r\nand\r\nnewlines")
	specialValue := types.Value("value\nwith\ttabs\r\nand\r\nnewlines")
	if err := pm.Put(specialKey, specialValue); err != nil {
		t.Errorf("Failed to put special characters: %v", err)
	}

	if value, err := pm.Get(specialKey); err != nil {
		t.Errorf("Failed to get special characters: %v", err)
	} else if string(value) != string(specialValue) {
		t.Errorf("Special characters mismatch")
	}

	overwriteKey := types.Key("overwrite_key")
	for i := 0; i < 10; i++ {
		value := types.Value(fmt.Sprintf("overwrite_value_%d", i))
		if err := pm.Put(overwriteKey, value); err != nil {
			t.Errorf("Failed to overwrite key %d: %v", i, err)
		}
	}

	expectedValue := types.Value("overwrite_value_9")
	if value, err := pm.Get(overwriteKey); err != nil {
		t.Errorf("Failed to get overwritten key: %v", err)
	} else if string(value) != string(expectedValue) {
		t.Errorf("Overwrite value mismatch: expected %s, got %s", expectedValue, value)
	}
}

func TestPartitionMemtableFlush(t *testing.T) {
	dataDir := "test_data_memtable"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}
	defer func() { _ = pm.Close() }()

	numKeys := 2000
	expectedData := make(map[types.Key]types.Value)

	for i := 0; i < numKeys; i++ {
		key := types.Key(fmt.Sprintf("memtable_key_%d", i))
		value := types.Value(fmt.Sprintf("memtable_value_%d", i))

		if err := pm.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
		expectedData[key] = value
	}

	for key, expectedValue := range expectedData {
		actualValue, err := pm.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}
		if string(actualValue) != string(expectedValue) {
			t.Fatalf("Value mismatch for key %s: expected %s, got %s", key, expectedValue, actualValue)
		}
	}

	stats := pm.GetStats()
	t.Logf("Memtable flush stats: %v", stats)
}
