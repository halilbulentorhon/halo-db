package partition

import (
	"fmt"
	"halo-db/pkg/store"
	"halo-db/pkg/types"
	"sync"
)

type Partition interface {
	Put(key types.Key, value types.Value) error
	Get(key types.Key) (types.Value, error)
	Delete(key types.Key) error
	List() []types.Key
	Clear() error
	Close() error
	GetID() int
}

type partition struct {
	ID    int
	store store.Store
	mu    sync.RWMutex
}

func NewPartition(id int, dataDir string) (Partition, error) {
	partitionDataDir := fmt.Sprintf("%s/partition_%d", dataDir, id)
	st, err := store.NewStore(partitionDataDir)
	if err != nil {
		return nil, err
	}

	return &partition{
		ID:    id,
		store: st,
	}, nil
}

func (p *partition) Put(key types.Key, value types.Value) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.store.Put(key, value)
}

func (p *partition) Get(key types.Key) (types.Value, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.store.Get(key)
}

func (p *partition) Delete(key types.Key) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.store.Delete(key)
}

func (p *partition) List() []types.Key {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.store.List()
}

func (p *partition) Clear() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.store.Clear()
}

func (p *partition) Close() error {
	return p.store.Close()
}

func (p *partition) GetID() int {
	return p.ID
}
