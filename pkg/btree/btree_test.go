package btree

import (
	"halo-db/pkg/types"
	"testing"
)

func TestBasicInsertAndFind(t *testing.T) {
	tree := NewBPlusTree()

	err := tree.Insert("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	value, err := tree.Find("key1")
	if err != nil {
		t.Fatalf("Failed to find key: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Expected value1, got %v", value)
	}

	_, err = tree.Find("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent key")
	}
}

func TestDuplicateKey(t *testing.T) {
	tree := NewBPlusTree()

	err := tree.Insert("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	err = tree.Insert("key1", []byte("value2"))
	if err != nil {
		t.Fatalf("Failed to overwrite key: %v", err)
	}

	value, err := tree.Find("key1")
	if err != nil {
		t.Fatalf("Failed to find key: %v", err)
	}
	if string(value) != "value2" {
		t.Errorf("Expected value2 after overwrite, got %v", value)
	}
}

func TestMultipleInserts(t *testing.T) {
	tree := NewBPlusTree()

	keys := []types.Key{"a", "b", "c", "d", "e"}
	values := []types.Value{[]byte("val1"), []byte("val2"), []byte("val3"), []byte("val4"), []byte("val5")}

	for i, key := range keys {
		err := tree.Insert(key, values[i])
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", key, err)
		}
	}

	for i, key := range keys {
		value, err := tree.Find(key)
		if err != nil {
			t.Fatalf("Failed to find %s: %v", key, err)
		}
		if string(value) != string(values[i]) {
			t.Errorf("Expected %s for key %s, got %s", string(values[i]), key, string(value))
		}
	}
}

func TestDelete(t *testing.T) {
	tree := NewBPlusTree()

	err := tree.Insert("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	value, err := tree.Find("key1")
	if err != nil {
		t.Fatalf("Failed to find key: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Expected value1, got %v", value)
	}

	err = tree.Delete("key1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	_, err = tree.Find("key1")
	if err == nil {
		t.Error("Expected error for deleted key")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	tree := NewBPlusTree()

	err := tree.Delete("nonexistent")
	if err == nil {
		t.Error("Expected error for deleting non-existent key")
	}
}
