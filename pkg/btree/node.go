package btree

import (
	"halo-db/pkg/constants"
	"halo-db/pkg/types"
)

type node struct {
	isLeaf   bool
	keys     []types.Key
	values   []types.Value
	children []*node
	next     *node
	parent   *node
}

func (n *node) IsFull() bool {
	return len(n.keys) >= constants.MaxKeys
}

func (n *node) InsertIntoNode(leftIndex int, key types.Key, right *node) error {
	n.shiftKeysAndChildren(leftIndex)
	n.keys[leftIndex] = key
	n.children[leftIndex+1] = right
	right.parent = n
	return nil
}

func (n *node) InsertKeyValue(key types.Key, value types.Value) {
	if !n.isLeaf {
		return
	}

	insertPosition := n.findInsertPosition(key)

	if insertPosition < len(n.keys) && n.keys[insertPosition] == key {
		n.values[insertPosition] = value
		return
	}

	n.insertAtPosition(insertPosition, key, value)
}

func (n *node) GetValue(key types.Key) (types.Value, bool) {
	if !n.isLeaf {
		return nil, false
	}

	for i, existingKey := range n.keys {
		if existingKey == key {
			return n.values[i], true
		}
	}
	return nil, false
}

func (n *node) DeleteKey(key types.Key) bool {
	if !n.isLeaf {
		return false
	}

	for i, existingKey := range n.keys {
		if existingKey == key {
			n.removeAtPosition(i)
			return true
		}
	}
	return false
}

func (n *node) FindChildIndex(key types.Key) int {
	childIndex := 0
	for childIndex < len(n.keys) && key >= n.keys[childIndex] {
		childIndex++
	}
	return childIndex
}

func (n *node) SplitWithKey(key types.Key, value types.Value) (*node, types.Key) {
	if !n.isLeaf {
		return nil, ""
	}

	tempKeys, tempValues := n.prepareTempArrays(key, value)
	splitPoint := n.calculateSplitPoint(len(tempKeys))

	n.updateWithLeftHalf(tempKeys, tempValues, splitPoint)
	newLeaf := n.createNewLeaf(tempKeys, tempValues, splitPoint)

	return newLeaf, newLeaf.keys[0]
}

func (n *node) SplitInternalWithKey(leftIndex int, key types.Key, right *node) (*node, types.Key) {
	if n.isLeaf {
		return nil, ""
	}

	tempKeys, tempChildren := n.prepareInternalTempArrays(leftIndex, key, right)
	splitPoint := n.calculateSplitPoint(len(tempKeys))
	promotedKey := tempKeys[splitPoint-1]

	n.updateInternalWithLeftHalf(tempKeys, tempChildren, splitPoint)
	newNode := n.createNewInternalNode(tempKeys, tempChildren, splitPoint)

	return newNode, promotedKey
}

func (n *node) GetLeftIndex(parent, left *node) int {
	leftIndex := 0
	for leftIndex <= len(parent.keys) && parent.children[leftIndex] != left {
		leftIndex++
	}
	return leftIndex
}

func (n *node) findInsertPosition(key types.Key) int {
	insertPosition := 0
	for insertPosition < len(n.keys) && n.keys[insertPosition] < key {
		insertPosition++
	}
	return insertPosition
}

func (n *node) insertAtPosition(position int, key types.Key, value types.Value) {
	n.keys = append(n.keys, "")
	n.values = append(n.values, nil)

	copy(n.keys[position+1:], n.keys[position:])
	copy(n.values[position+1:], n.values[position:])

	n.keys[position] = key
	n.values[position] = value
}

func (n *node) removeAtPosition(position int) {
	n.keys = append(n.keys[:position], n.keys[position+1:]...)
	n.values = append(n.values[:position], n.values[position+1:]...)
}

func (n *node) prepareTempArrays(key types.Key, value types.Value) ([]types.Key, []types.Value) {
	keyCount := len(n.keys)
	tempKeys := make([]types.Key, keyCount+1)
	tempValues := make([]types.Value, keyCount+1)

	keyInserted := false
	tempIndex := 0

	for i := 0; i < keyCount; i++ {
		if !keyInserted && key < n.keys[i] {
			tempKeys[tempIndex] = key
			tempValues[tempIndex] = value
			tempIndex++
			keyInserted = true
		}
		tempKeys[tempIndex] = n.keys[i]
		tempValues[tempIndex] = n.values[i]
		tempIndex++
	}

	if !keyInserted {
		tempKeys[tempIndex] = key
		tempValues[tempIndex] = value
	}

	return tempKeys, tempValues
}

func (n *node) calculateSplitPoint(totalKeys int) int {
	return totalKeys / 2
}

func (n *node) updateWithLeftHalf(tempKeys []types.Key, tempValues []types.Value, splitPoint int) {
	n.keys = append([]types.Key{}, tempKeys[:splitPoint]...)
	n.values = append([]types.Value{}, tempValues[:splitPoint]...)
}

func (n *node) createNewLeaf(tempKeys []types.Key, tempValues []types.Value, splitPoint int) *node {
	newLeaf := &node{isLeaf: true}
	newLeaf.keys = append([]types.Key{}, tempKeys[splitPoint:]...)
	newLeaf.values = append([]types.Value{}, tempValues[splitPoint:]...)
	newLeaf.next = n.next
	newLeaf.parent = n.parent
	n.next = newLeaf

	return newLeaf
}

func (n *node) prepareInternalTempArrays(leftIndex int, key types.Key, right *node) ([]types.Key, []*node) {
	keyCount := len(n.keys)
	tempKeys := make([]types.Key, keyCount+1)
	tempChildren := make([]*node, keyCount+2)

	n.copyChildrenToTemp(tempChildren, leftIndex, right)
	n.copyKeysToTemp(tempKeys, leftIndex, key)

	return tempKeys, tempChildren
}

func (n *node) copyChildrenToTemp(tempChildren []*node, leftIndex int, right *node) {
	for i, j := 0, 0; i < len(n.children)+1; i++ {
		if i == leftIndex+1 {
			tempChildren[j] = right
			j++
		}
		if i < len(n.children) {
			tempChildren[j] = n.children[i]
			j++
		}
	}
}

func (n *node) copyKeysToTemp(tempKeys []types.Key, leftIndex int, key types.Key) {
	for i, j := 0, 0; i < len(n.keys)+1; i++ {
		if i == leftIndex {
			tempKeys[j] = key
			j++
		}
		if i < len(n.keys) {
			tempKeys[j] = n.keys[i]
			j++
		}
	}
}

func (n *node) updateInternalWithLeftHalf(tempKeys []types.Key, tempChildren []*node, splitPoint int) {
	n.keys = append([]types.Key{}, tempKeys[:splitPoint-1]...)
	n.children = append([]*node{}, tempChildren[:splitPoint]...)
}

func (n *node) createNewInternalNode(tempKeys []types.Key, tempChildren []*node, splitPoint int) *node {
	newNode := &node{isLeaf: false}
	newNode.keys = append([]types.Key{}, tempKeys[splitPoint:]...)
	newNode.children = append([]*node{}, tempChildren[splitPoint:]...)
	newNode.parent = n.parent

	n.updateChildrenParent(newNode)

	return newNode
}

func (n *node) updateChildrenParent(newNode *node) {
	for _, child := range newNode.children {
		if child != nil {
			child.parent = newNode
		}
	}
}

func (n *node) shiftKeysAndChildren(leftIndex int) {
	n.keys = append(n.keys, "")
	n.children = append(n.children, nil)

	for i := len(n.keys) - 1; i > leftIndex; i-- {
		n.keys[i] = n.keys[i-1]
		n.children[i+1] = n.children[i]
	}
}
