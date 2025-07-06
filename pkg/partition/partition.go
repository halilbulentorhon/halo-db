package partition

import (
	"fmt"
	"halo-db/pkg/btree"
	"halo-db/pkg/constants"
	"halo-db/pkg/memtable"
	"halo-db/pkg/types"
	"halo-db/pkg/wal"
	"sync"
)

type Partition struct {
	ID       int
	Tree     *btree.BPlusTree
	Memtable *memtable.Memtable
	WAL      *wal.WAL
	mu       sync.RWMutex
}

func NewPartition(id int, dataDir string) (*Partition, error) {
	tree := btree.NewBPlusTree()
	memtable := memtable.NewMemtable(constants.MemtableSize)
	partitionDataDir := fmt.Sprintf("%s/partition_%d", dataDir, id)
	walPath := partitionDataDir
	wal, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	return &Partition{
		ID:       id,
		Tree:     tree,
		Memtable: memtable,
		WAL:      wal,
	}, nil
}

func (p *Partition) flushMemtable() error {
	entries := p.Memtable.GetAllEntries()

	p.Tree.SkipBloomFilter(true)
	defer p.Tree.SkipBloomFilter(false)

	for _, entry := range entries {
		if err := p.Tree.Insert(entry.Key, entry.Value); err != nil {
			return err
		}
		p.Tree.BloomFilter.Add(string(entry.Key))
	}

	p.Memtable.Clear()
	return nil
}

func (p *Partition) List() []types.Key {
	var keys []types.Key

	if p.Tree.Root != nil {
		keys = append(keys, p.collectAllKeys(p.Tree.Root)...)
	}

	for _, entry := range p.Memtable.GetAllEntries() {
		keys = append(keys, entry.Key)
	}

	return keys
}

func (p *Partition) collectAllKeys(node *btree.Node) []types.Key {
	var keys []types.Key

	if node.IsLeaf {
		keys = append(keys, node.Keys...)
	} else {
		for _, child := range node.Children {
			keys = append(keys, p.collectAllKeys(child)...)
		}
	}

	return keys
}

func (p *Partition) replayWAL() error {
	return p.WAL.Replay(
		func(key types.Key, value types.Value) error {
			return p.Tree.Insert(key, value)
		},
		func(key types.Key) error {
			return p.Tree.Delete(key)
		},
	)
}
