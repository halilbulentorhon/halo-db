package btree

import (
	"errors"
	"halo-db/pkg/bloom"
	"halo-db/pkg/types"
)

var ErrKeyNotFound = errors.New("key not found")

type BPlusTree struct {
	Root        *Node
	BloomFilter *bloom.BloomFilter
	skipBloom   bool
}

func NewBPlusTree() *BPlusTree {
	size := bloom.EstimateSize(1000, 0.01)
	hashFuncs := bloom.EstimateHashFunctions(size, 1000)
	return &BPlusTree{
		BloomFilter: bloom.NewBloomFilter(size, hashFuncs),
	}
}

func (t *BPlusTree) SkipBloomFilter(skip bool) {
	t.skipBloom = skip
}

func (t *BPlusTree) Insert(key types.Key, value types.Value) error {
	if t.Root == nil {
		t.Root = newLeafNode()
		t.Root.InsertKeyValue(key, value)
		t.BloomFilter.Add(string(key))
		return nil
	}

	leaf := t.findLeaf(key)
	if leaf == nil {
		return errors.New("failed to find leaf")
	}

	if !leaf.IsFull() {
		leaf.InsertKeyValue(key, value)
		t.BloomFilter.Add(string(key))
		return nil
	}

	if err := t.insertIntoLeafAfterSplitting(leaf, key, value); err != nil {
		return err
	}
	t.BloomFilter.Add(string(key))
	return nil
}

func (t *BPlusTree) Find(key types.Key) (types.Value, error) {
	if !t.skipBloom && !t.BloomFilter.Contains(string(key)) {
		return nil, ErrKeyNotFound
	}
	if t.Root == nil {
		return nil, ErrKeyNotFound
	}
	leaf := t.findLeaf(key)
	if leaf == nil {
		return nil, ErrKeyNotFound
	}

	value, found := leaf.GetValue(key)
	if !found {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

func (t *BPlusTree) Delete(key types.Key) error {
	if t.Root == nil {
		return ErrKeyNotFound
	}
	leaf := t.findLeaf(key)
	if leaf == nil {
		return ErrKeyNotFound
	}

	if leaf.DeleteKey(key) {
		t.BloomFilter.Clear()
		return nil
	}
	return ErrKeyNotFound
}

func (t *BPlusTree) findLeaf(key types.Key) *Node {
	if t.Root == nil {
		return nil
	}
	current := t.Root
	for !current.IsLeaf {
		childIndex := current.FindChildIndex(key)
		current = current.Children[childIndex]
	}
	return current
}

func (t *BPlusTree) insertIntoLeafAfterSplitting(leaf *Node, key types.Key, value types.Value) error {
	newLeaf, promotedKey := leaf.SplitWithKey(key, value)
	if newLeaf == nil {
		return errors.New("failed to split leaf")
	}

	return t.insertIntoParent(leaf, promotedKey, newLeaf)
}

func (t *BPlusTree) insertIntoParent(left *Node, key types.Key, right *Node) error {
	if left.Parent == nil {
		return t.insertIntoNewRoot(left, key, right)
	}

	parent := left.Parent
	leftIndex := parent.GetLeftIndex(parent, left)

	if !parent.IsFull() {
		return parent.InsertIntoNode(leftIndex, key, right)
	}

	return t.insertIntoNodeAfterSplitting(parent, leftIndex, key, right)
}

func (t *BPlusTree) insertIntoNewRoot(left *Node, key types.Key, right *Node) error {
	t.Root = &Node{
		IsLeaf:   false,
		Keys:     []types.Key{key},
		Children: []*Node{left, right},
	}
	left.Parent = t.Root
	right.Parent = t.Root
	return nil
}

func (t *BPlusTree) insertIntoNodeAfterSplitting(oldNode *Node, leftIndex int, key types.Key, right *Node) error {
	newNode, promotedKey := oldNode.SplitInternalWithKey(leftIndex, key, right)
	if newNode == nil {
		return errors.New("failed to split internal node")
	}

	return t.insertIntoParent(oldNode, promotedKey, newNode)
}

func newLeafNode() *Node {
	return &Node{IsLeaf: true}
}
