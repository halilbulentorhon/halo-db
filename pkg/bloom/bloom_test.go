package bloom

import (
	"testing"
)

func TestBloomFilterAddAndContains(t *testing.T) {
	bf := NewBloomFilter(1000, 5)

	keys := []string{"key1", "key2", "key3", "apple", "banana"}

	for _, key := range keys {
		bf.Add(key)
		if !bf.Contains(key) {
			t.Errorf("Bloom filter should contain key: %s", key)
		}
	}
}

func TestBloomFilterFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(1000, 5)

	keys := []string{"key1", "key2", "key3"}
	for _, key := range keys {
		bf.Add(key)
	}

	for _, key := range keys {
		if !bf.Contains(key) {
			t.Errorf("Bloom filter should not have false negatives for: %s", key)
		}
	}
}

func TestBloomFilterFalsePositives(t *testing.T) {
	bf := NewBloomFilter(100, 3)

	addedKeys := []string{"key1", "key2", "key3"}
	for _, key := range addedKeys {
		bf.Add(key)
	}

	notAddedKeys := []string{"key4", "key5", "key6", "different_key"}
	falsePositives := 0

	for _, key := range notAddedKeys {
		if bf.Contains(key) {
			falsePositives++
		}
	}

	if falsePositives == len(notAddedKeys) {
		t.Error("Bloom filter should not have 100% false positive rate")
	}
}

func TestBloomFilterClear(t *testing.T) {
	bf := NewBloomFilter(1000, 5)

	bf.Add("key1")
	if !bf.Contains("key1") {
		t.Error("Bloom filter should contain key1 after adding")
	}

	bf.Clear()
	if bf.Contains("key1") {
		t.Error("Bloom filter should not contain key1 after clearing")
	}
}

func TestBloomFilterEstimateSize(t *testing.T) {
	size := EstimateSize(1000, 0.01)
	if size == 0 {
		t.Error("Estimated size should not be zero")
	}

	size2 := EstimateSize(1000, 0.1)
	if size2 >= size {
		t.Error("Higher false positive rate should result in smaller size")
	}
}

func TestBloomFilterEstimateHashFunctions(t *testing.T) {
	hashFuncs := EstimateHashFunctions(1000, 100)
	if hashFuncs == 0 {
		t.Error("Estimated hash functions should not be zero")
	}
}
