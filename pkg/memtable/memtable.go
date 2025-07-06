package memtable

import (
	"halo-db/pkg/constants"
	"halo-db/pkg/types"
	"sort"
	"sync"
)

type Entry struct {
	Key   types.Key
	Value types.Value
}

type Memtable interface {
	Put(key types.Key, value types.Value)
	Get(key types.Key) (types.Value, bool)
	Delete(key types.Key)
	GetAllEntries() []Entry
	GetSize() int
	IsFull() bool
	Clear()
}

type memtable struct {
	entries []Entry
	size    int
	mu      sync.RWMutex
}

func NewMemtable(size int) Memtable {
	if size <= 0 {
		size = constants.MemtableSize
	}
	return &memtable{
		entries: make([]Entry, 0),
		size:    size,
	}
}

func (m *memtable) Put(key types.Key, value types.Value) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pos := sort.Search(len(m.entries), func(i int) bool {
		return m.entries[i].Key >= key
	})

	if pos < len(m.entries) && m.entries[pos].Key == key {
		m.entries[pos].Value = value
		return
	}

	m.entries = append(m.entries, Entry{})
	copy(m.entries[pos+1:], m.entries[pos:])
	m.entries[pos] = Entry{Key: key, Value: value}
}

func (m *memtable) Get(key types.Key) (types.Value, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pos := sort.Search(len(m.entries), func(i int) bool {
		return m.entries[i].Key >= key
	})

	if pos < len(m.entries) && m.entries[pos].Key == key {
		return m.entries[pos].Value, true
	}

	return nil, false
}

func (m *memtable) Delete(key types.Key) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pos := sort.Search(len(m.entries), func(i int) bool {
		return m.entries[i].Key >= key
	})

	if pos < len(m.entries) && m.entries[pos].Key == key {
		m.entries = append(m.entries[:pos], m.entries[pos+1:]...)
	}
}

func (m *memtable) GetAllEntries() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Entry, len(m.entries))
	copy(result, m.entries)
	return result
}

func (m *memtable) GetSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

func (m *memtable) IsFull() bool {
	return m.GetSize() >= m.size
}

func (m *memtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make([]Entry, 0)
}
