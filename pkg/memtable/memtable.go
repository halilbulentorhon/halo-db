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

type Memtable struct {
	entries []Entry
	size    int
	mu      sync.RWMutex
}

func NewMemtable(size int) *Memtable {
	if size <= 0 {
		size = constants.MemtableSize
	}
	return &Memtable{
		entries: make([]Entry, 0),
		size:    size,
	}
}

func (m *Memtable) Put(key types.Key, value types.Value) {
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

func (m *Memtable) Get(key types.Key) (types.Value, bool) {
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

func (m *Memtable) Delete(key types.Key) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pos := sort.Search(len(m.entries), func(i int) bool {
		return m.entries[i].Key >= key
	})

	if pos < len(m.entries) && m.entries[pos].Key == key {
		m.entries = append(m.entries[:pos], m.entries[pos+1:]...)
	}
}

func (m *Memtable) GetAllEntries() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Entry, len(m.entries))
	copy(result, m.entries)
	return result
}

func (m *Memtable) GetSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

func (m *Memtable) IsFull() bool {
	return m.GetSize() >= m.size
}

func (m *Memtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make([]Entry, 0)
}
