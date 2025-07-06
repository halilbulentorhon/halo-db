package btree

import (
	"errors"
	"halo-db/pkg/bloom"
	"halo-db/pkg/types"
)

var ErrKeyNotFound = errors.New("key not found")

type BTree interface {
	Insert(key types.Key, value types.Value) error
	Find(key types.Key) (types.Value, error)
	Delete(key types.Key) error
	SkipBloomFilter(skip bool)
	List() []types.Key
}

type bPlusTree struct {
	root        *node
	bloomFilter *bloom.BloomFilter
	skipBloom   bool
}

func NewBPlusTree() BTree {
	size := bloom.EstimateSize(1000, 0.01)
	hashFuncs := bloom.EstimateHashFunctions(size, 1000)
	return &bPlusTree{
		bloomFilter: bloom.NewBloomFilter(size, hashFuncs),
	}
}

func (t *bPlusTree) SkipBloomFilter(skip bool) {
	t.skipBloom = skip
}

func (t *bPlusTree) Insert(key types.Key, value types.Value) error {
	if t.root == nil {
		t.root = newLeafNode()
		t.root.InsertKeyValue(key, value)
		t.bloomFilter.Add(key)
		return nil
	}

	leaf := t.findLeaf(key)
	if leaf == nil {
		return errors.New("failed to find leaf")
	}

	if !leaf.IsFull() {
		leaf.InsertKeyValue(key, value)
		t.bloomFilter.Add(key)
		return nil
	}

	if err := t.insertIntoLeafAfterSplitting(leaf, key, value); err != nil {
		return err
	}
	t.bloomFilter.Add(key)
	return nil
}

func (t *bPlusTree) Find(key types.Key) (types.Value, error) {
	if !t.skipBloom && !t.bloomFilter.Contains(string(key)) {
		return nil, ErrKeyNotFound
	}
	if t.root == nil {
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

func (t *bPlusTree) Delete(key types.Key) error {
	if t.root == nil {
		return ErrKeyNotFound
	}
	leaf := t.findLeaf(key)
	if leaf == nil {
		return ErrKeyNotFound
	}

	if leaf.DeleteKey(key) {
		t.bloomFilter.Clear()
		return nil
	}
	return ErrKeyNotFound
}

func (t *bPlusTree) List() []types.Key {
	if t.root == nil {
		return []types.Key{}
	}
	return t.collectAllKeys(t.root)
}

func (t *bPlusTree) collectAllKeys(node *node) []types.Key {
	var keys []types.Key

	if node.isLeaf {
		keys = append(keys, node.keys...)
	} else {
		for _, child := range node.children {
			keys = append(keys, t.collectAllKeys(child)...)
		}
	}

	return keys
}

func (t *bPlusTree) findLeaf(key types.Key) *node {
	if t.root == nil {
		return nil
	}
	current := t.root
	for !current.isLeaf {
		childIndex := current.FindChildIndex(key)
		current = current.children[childIndex]
	}
	return current
}

func (t *bPlusTree) insertIntoLeafAfterSplitting(leaf *node, key types.Key, value types.Value) error {
	newLeaf, promotedKey := leaf.SplitWithKey(key, value)
	if newLeaf == nil {
		return errors.New("failed to split leaf")
	}

	return t.insertIntoParent(leaf, promotedKey, newLeaf)
}

func (t *bPlusTree) insertIntoParent(left *node, key types.Key, right *node) error {
	if left.parent == nil {
		return t.insertIntoNewRoot(left, key, right)
	}

	parent := left.parent
	leftIndex := parent.GetLeftIndex(parent, left)

	if !parent.IsFull() {
		return parent.InsertIntoNode(leftIndex, key, right)
	}

	return t.insertIntoNodeAfterSplitting(parent, leftIndex, key, right)
}

func (t *bPlusTree) insertIntoNewRoot(left *node, key types.Key, right *node) error {
	t.root = &node{
		isLeaf:   false,
		keys:     []types.Key{key},
		children: []*node{left, right},
	}
	left.parent = t.root
	right.parent = t.root
	return nil
}

func (t *bPlusTree) insertIntoNodeAfterSplitting(oldNode *node, leftIndex int, key types.Key, right *node) error {
	newNode, promotedKey := oldNode.SplitInternalWithKey(leftIndex, key, right)
	if newNode == nil {
		return errors.New("failed to split internal node")
	}

	return t.insertIntoParent(oldNode, promotedKey, newNode)
}

func newLeafNode() *node {
	return &node{isLeaf: true}
}
