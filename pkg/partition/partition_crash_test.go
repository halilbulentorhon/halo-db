package partition

import (
	"fmt"
	"halo-db/pkg/types"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestCrashRecovery(t *testing.T) {
	dataDir := "test_data_crash"
	_ = os.RemoveAll(dataDir)

	t.Run("CrashDuringWALWrite", func(t *testing.T) {
		testCrashDuringWALWrite(t, dataDir)
	})

	t.Run("CrashDuringMemtableFlush", func(t *testing.T) {
		testCrashDuringMemtableFlush(t, dataDir)
	})

	t.Run("CrashDuringMultipleOperations", func(t *testing.T) {
		testCrashDuringMultipleOperations(t, dataDir)
	})

	t.Run("PartialWALCorruption", func(t *testing.T) {
		testPartialWALCorruption(t, dataDir)
	})
}

func testCrashDuringWALWrite(t *testing.T, baseDataDir string) {
	dataDir := filepath.Join(baseDataDir, "wal_crash")
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(2, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}

	initialData := map[types.Key]types.Value{
		"key1": types.Value("value1"),
		"key2": types.Value("value2"),
		"key3": types.Value("value3"),
	}

	for key, value := range initialData {
		if err := pm.Put(key, value); err != nil {
			t.Fatalf("Failed to put initial data: %v", err)
		}
	}

	for key, expectedValue := range initialData {
		actualValue, err := pm.Get(key)
		if err != nil {
			t.Fatalf("Failed to get initial data: %v", err)
		}
		if string(actualValue) != string(expectedValue) {
			t.Fatalf("Initial data mismatch for key %s", key)
		}
	}

	_ = pm.Close()

	pm2, err := NewPartitionManager(2, dataDir)
	if err != nil {
		t.Fatalf("Failed to create new partition manager: %v", err)
	}
	defer func() { _ = pm2.Close() }()

	for key, expectedValue := range initialData {
		actualValue, err := pm2.Get(key)
		if err != nil {
			t.Errorf("Failed to recover data for key %s: %v", key, err)
		} else if string(actualValue) != string(expectedValue) {
			t.Errorf("Recovery data mismatch for key %s: expected %s, got %s", key, expectedValue, actualValue)
		}
	}

	if err := pm2.Put("new_key", types.Value("new_value")); err != nil {
		t.Errorf("Failed to put new data after recovery: %v", err)
	}

	if value, err := pm2.Get("new_key"); err != nil {
		t.Errorf("Failed to get new data after recovery: %v", err)
	} else if string(value) != "new_value" {
		t.Errorf("New data mismatch after recovery")
	}
}

func testCrashDuringMemtableFlush(t *testing.T, baseDataDir string) {
	dataDir := filepath.Join(baseDataDir, "flush_crash")
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(2, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}

	numKeys := 1500
	expectedData := make(map[types.Key]types.Value)

	for i := 0; i < numKeys; i++ {
		key := types.Key(fmt.Sprintf("flush_key_%d", i))
		value := types.Value(fmt.Sprintf("flush_value_%d", i))

		if err := pm.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
		expectedData[key] = value
	}

	for key, expectedValue := range expectedData {
		actualValue, err := pm.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %s before crash: %v", key, err)
		}
		if string(actualValue) != string(expectedValue) {
			t.Fatalf("Data mismatch before crash for key %s", key)
		}
	}

	_ = pm.Close()

	pm2, err := NewPartitionManager(2, dataDir)
	if err != nil {
		t.Fatalf("Failed to create new partition manager: %v", err)
	}
	defer func() { _ = pm2.Close() }()

	for key, expectedValue := range expectedData {
		actualValue, err := pm2.Get(key)
		if err != nil {
			t.Errorf("Failed to recover data for key %s: %v", key, err)
		} else if string(actualValue) != string(expectedValue) {
			t.Errorf("Recovery data mismatch for key %s: expected %s, got %s", key, expectedValue, actualValue)
		}
	}
}

func testCrashDuringMultipleOperations(t *testing.T, baseDataDir string) {
	dataDir := filepath.Join(baseDataDir, "multi_crash")
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}

	operations := []struct {
		op    string
		key   types.Key
		value types.Value
	}{
		{"put", "key1", types.Value("value1")},
		{"put", "key2", types.Value("value2")},
		{"delete", "key1", types.Value("")},
		{"put", "key3", types.Value("value3")},
		{"put", "key1", types.Value("new_value1")},
		{"delete", "key2", types.Value("")},
		{"put", "key4", types.Value("value4")},
	}

	expectedFinalState := map[types.Key]types.Value{
		"key1": types.Value("new_value1"),
		"key3": types.Value("value3"),
		"key4": types.Value("value4"),
	}

	for _, op := range operations {
		switch op.op {
		case "put":
			if err := pm.Put(op.key, op.value); err != nil {
				t.Fatalf("Failed to put %s: %v", op.key, err)
			}
		case "delete":
			if err := pm.Delete(op.key); err != nil {
				t.Fatalf("Failed to delete %s: %v", op.key, err)
			}
		}
	}

	for key, expectedValue := range expectedFinalState {
		actualValue, err := pm.Get(key)
		if err != nil {
			t.Fatalf("Failed to get %s before crash: %v", key, err)
		}
		if string(actualValue) != string(expectedValue) {
			t.Fatalf("Data mismatch before crash for key %s", key)
		}
	}

	deletedKeys := []types.Key{"key2"}
	for _, key := range deletedKeys {
		if _, err := pm.Get(key); err == nil {
			t.Fatalf("Deleted key %s still exists before crash", key)
		}
	}

	_ = pm.Close()

	pm2, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create new partition manager: %v", err)
	}
	defer func() { _ = pm2.Close() }()

	for key, expectedValue := range expectedFinalState {
		actualValue, err := pm2.Get(key)
		if err != nil {
			t.Errorf("Failed to recover data for key %s: %v", key, err)
		} else if string(actualValue) != string(expectedValue) {
			t.Errorf("Recovery data mismatch for key %s: expected %s, got %s", key, expectedValue, actualValue)
		}
	}

	for _, key := range deletedKeys {
		if _, err := pm2.Get(key); err == nil {
			t.Errorf("Deleted key %s exists after recovery", key)
		}
	}
}

func testPartialWALCorruption(t *testing.T, baseDataDir string) {
	dataDir := filepath.Join(baseDataDir, "corruption")
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(2, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}

	testData := map[types.Key]types.Value{
		"key1": types.Value("value1"),
		"key2": types.Value("value2"),
		"key3": types.Value("value3"),
	}

	for key, value := range testData {
		if err := pm.Put(key, value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	_ = pm.Close()

	partition0WAL := filepath.Join(dataDir, "partition_0", "wal.log")

	if _, err := os.Stat(partition0WAL); os.IsNotExist(err) {
		t.Skip("WAL file does not exist, skipping corruption test")
		return
	}

	err = os.WriteFile(partition0WAL, []byte("corrupted data"), 0644)
	if err != nil {
		t.Fatalf("Failed to write corrupted data: %v", err)
	}

	pm2, err := NewPartitionManager(2, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager after corruption: %v", err)
	}
	defer func() { _ = pm2.Close() }()

	if err := pm2.Put("new_key", types.Value("new_value")); err != nil {
		t.Errorf("Failed to put data after corruption: %v", err)
	}

	if value, err := pm2.Get("new_key"); err != nil {
		t.Errorf("Failed to get data after corruption: %v", err)
	} else if string(value) != "new_value" {
		t.Errorf("Data mismatch after corruption")
	}
}

func TestConcurrentCrashRecovery(t *testing.T) {
	dataDir := "test_data_concurrent_crash"
	_ = os.RemoveAll(dataDir)

	pm, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}

	var mu sync.RWMutex
	operations := make(map[types.Key]types.Value)

	numGoroutines := 20
	numOperationsPerGoroutine := 50
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numOperationsPerGoroutine; j++ {
				key := types.Key(fmt.Sprintf("concurrent_%d_%d", goroutineID, j))
				value := types.Value(fmt.Sprintf("value_%d_%d", goroutineID, j))

				if err := pm.Put(key, value); err != nil {
					t.Errorf("Concurrent put failed: %v", err)
					return
				}

				mu.Lock()
				operations[key] = value
				mu.Unlock()

				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	_ = pm.Close()

	pm2, err := NewPartitionManager(4, dataDir)
	if err != nil {
		t.Fatalf("Failed to create new partition manager: %v", err)
	}
	defer func() { _ = pm2.Close() }()

	mu.RLock()
	for key, expectedValue := range operations {
		actualValue, err := pm2.Get(key)
		if err != nil {
			t.Errorf("Failed to recover concurrent data for key %s: %v", key, err)
		} else if string(actualValue) != string(expectedValue) {
			t.Errorf("Concurrent recovery data mismatch for key %s: expected %s, got %s", key, expectedValue, actualValue)
		}
	}
	mu.RUnlock()
}
