package memtable

import (
	"halo-db/pkg/types"
	"testing"
)

func TestMemtablePutAndGet(t *testing.T) {
	mt := NewMemtable(100)

	mt.Put("key1", types.Value("value1"))
	mt.Put("key2", types.Value("value2"))

	if value, found := mt.Get("key1"); !found || string(value) != "value1" {
		t.Errorf("Expected value1, got %v", value)
	}

	if value, found := mt.Get("key2"); !found || string(value) != "value2" {
		t.Errorf("Expected value2, got %v", value)
	}

	if _, found := mt.Get("key3"); found {
		t.Error("Expected key3 not found")
	}
}

func TestMemtableOverwrite(t *testing.T) {
	mt := NewMemtable(100)

	mt.Put("key1", types.Value("value1"))
	mt.Put("key1", types.Value("value1_updated"))

	if value, found := mt.Get("key1"); !found || string(value) != "value1_updated" {
		t.Errorf("Expected value1_updated, got %v", value)
	}
}

func TestMemtableDelete(t *testing.T) {
	mt := NewMemtable(100)

	mt.Put("key1", types.Value("value1"))
	mt.Delete("key1")

	if value, found := mt.Get("key1"); found && value != nil {
		t.Errorf("Expected deleted key to return nil, got %v", value)
	}
}

func TestMemtableSortedOrder(t *testing.T) {
	mt := NewMemtable(100)

	mt.Put("zebra", types.Value("value3"))
	mt.Put("apple", types.Value("value1"))
	mt.Put("banana", types.Value("value2"))

	entries := mt.GetAllEntries()
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	if entries[0].Key != "apple" || entries[1].Key != "banana" || entries[2].Key != "zebra" {
		t.Errorf("Entries not in sorted order: %v", entries)
	}
}

func TestMemtableSize(t *testing.T) {
	mt := NewMemtable(100)

	if mt.GetSize() != 0 {
		t.Errorf("Expected size 0, got %d", mt.GetSize())
	}

	mt.Put("key1", types.Value("value1"))
	if mt.GetSize() != 1 {
		t.Errorf("Expected size 1, got %d", mt.GetSize())
	}

	mt.Put("key1", types.Value("value1_updated"))
	if mt.GetSize() != 1 {
		t.Errorf("Expected size 1 after overwrite, got %d", mt.GetSize())
	}
}

func TestMemtableIsFull(t *testing.T) {
	mt := NewMemtable(2)

	if mt.IsFull() {
		t.Error("Expected not full initially")
	}

	mt.Put("key1", types.Value("value1"))
	if mt.IsFull() {
		t.Error("Expected not full with 1 entry")
	}

	mt.Put("key2", types.Value("value2"))
	if !mt.IsFull() {
		t.Error("Expected full with 2 entries")
	}
}
