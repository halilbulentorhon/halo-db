package btree

import (
	"halo-db/pkg/constants"
	"halo-db/pkg/types"
)

type Node struct {
	IsLeaf   bool
	Keys     []types.Key
	Values   []types.Value
	Children []*Node
	Next     *Node
	Parent   *Node
}

func (n *Node) IsFull() bool {
	return len(n.Keys) >= constants.MaxKeys
}

func (n *Node) InsertKeyValue(key types.Key, value types.Value) {
	if !n.IsLeaf {
		return
	}

	insertPosition := n.findInsertPosition(key)

	if insertPosition < len(n.Keys) && n.Keys[insertPosition] == key {
		n.Values[insertPosition] = value
		return
	}

	n.insertAtPosition(insertPosition, key, value)
}

func (n *Node) findInsertPosition(key types.Key) int {
	insertPosition := 0
	for insertPosition < len(n.Keys) && n.Keys[insertPosition] < key {
		insertPosition++
	}
	return insertPosition
}

func (n *Node) insertAtPosition(position int, key types.Key, value types.Value) {
	n.Keys = append(n.Keys, "")
	n.Values = append(n.Values, nil)

	copy(n.Keys[position+1:], n.Keys[position:])
	copy(n.Values[position+1:], n.Values[position:])

	n.Keys[position] = key
	n.Values[position] = value
}

func (n *Node) GetValue(key types.Key) (types.Value, bool) {
	if !n.IsLeaf {
		return nil, false
	}

	for i, existingKey := range n.Keys {
		if existingKey == key {
			return n.Values[i], true
		}
	}
	return nil, false
}

func (n *Node) DeleteKey(key types.Key) bool {
	if !n.IsLeaf {
		return false
	}

	for i, existingKey := range n.Keys {
		if existingKey == key {
			n.removeAtPosition(i)
			return true
		}
	}
	return false
}

func (n *Node) removeAtPosition(position int) {
	n.Keys = append(n.Keys[:position], n.Keys[position+1:]...)
	n.Values = append(n.Values[:position], n.Values[position+1:]...)
}

func (n *Node) FindChildIndex(key types.Key) int {
	childIndex := 0
	for childIndex < len(n.Keys) && key >= n.Keys[childIndex] {
		childIndex++
	}
	return childIndex
}

func (n *Node) SplitWithKey(key types.Key, value types.Value) (*Node, types.Key) {
	if !n.IsLeaf {
		return nil, ""
	}

	tempKeys, tempValues := n.prepareTempArrays(key, value)
	splitPoint := n.calculateSplitPoint(len(tempKeys))

	n.updateWithLeftHalf(tempKeys, tempValues, splitPoint)
	newLeaf := n.createNewLeaf(tempKeys, tempValues, splitPoint)

	return newLeaf, newLeaf.Keys[0]
}

func (n *Node) prepareTempArrays(key types.Key, value types.Value) ([]types.Key, []types.Value) {
	keyCount := len(n.Keys)
	tempKeys := make([]types.Key, keyCount+1)
	tempValues := make([]types.Value, keyCount+1)

	keyInserted := false
	tempIndex := 0

	for i := 0; i < keyCount; i++ {
		if !keyInserted && key < n.Keys[i] {
			tempKeys[tempIndex] = key
			tempValues[tempIndex] = value
			tempIndex++
			keyInserted = true
		}
		tempKeys[tempIndex] = n.Keys[i]
		tempValues[tempIndex] = n.Values[i]
		tempIndex++
	}

	if !keyInserted {
		tempKeys[tempIndex] = key
		tempValues[tempIndex] = value
	}

	return tempKeys, tempValues
}

func (n *Node) calculateSplitPoint(totalKeys int) int {
	return totalKeys / 2
}

func (n *Node) updateWithLeftHalf(tempKeys []types.Key, tempValues []types.Value, splitPoint int) {
	n.Keys = append([]types.Key{}, tempKeys[:splitPoint]...)
	n.Values = append([]types.Value{}, tempValues[:splitPoint]...)
}

func (n *Node) createNewLeaf(tempKeys []types.Key, tempValues []types.Value, splitPoint int) *Node {
	newLeaf := &Node{IsLeaf: true}
	newLeaf.Keys = append([]types.Key{}, tempKeys[splitPoint:]...)
	newLeaf.Values = append([]types.Value{}, tempValues[splitPoint:]...)
	newLeaf.Next = n.Next
	newLeaf.Parent = n.Parent
	n.Next = newLeaf

	return newLeaf
}

func (n *Node) SplitInternalWithKey(leftIndex int, key types.Key, right *Node) (*Node, types.Key) {
	if n.IsLeaf {
		return nil, ""
	}

	tempKeys, tempChildren := n.prepareInternalTempArrays(leftIndex, key, right)
	splitPoint := n.calculateSplitPoint(len(tempKeys))
	promotedKey := tempKeys[splitPoint-1]

	n.updateInternalWithLeftHalf(tempKeys, tempChildren, splitPoint)
	newNode := n.createNewInternalNode(tempKeys, tempChildren, splitPoint)

	return newNode, promotedKey
}

func (n *Node) prepareInternalTempArrays(leftIndex int, key types.Key, right *Node) ([]types.Key, []*Node) {
	keyCount := len(n.Keys)
	tempKeys := make([]types.Key, keyCount+1)
	tempChildren := make([]*Node, keyCount+2)

	n.copyChildrenToTemp(tempChildren, leftIndex, right)
	n.copyKeysToTemp(tempKeys, leftIndex, key)

	return tempKeys, tempChildren
}

func (n *Node) copyChildrenToTemp(tempChildren []*Node, leftIndex int, right *Node) {
	for i, j := 0, 0; i < len(n.Children)+1; i++ {
		if i == leftIndex+1 {
			tempChildren[j] = right
			j++
		}
		if i < len(n.Children) {
			tempChildren[j] = n.Children[i]
			j++
		}
	}
}

func (n *Node) copyKeysToTemp(tempKeys []types.Key, leftIndex int, key types.Key) {
	for i, j := 0, 0; i < len(n.Keys)+1; i++ {
		if i == leftIndex {
			tempKeys[j] = key
			j++
		}
		if i < len(n.Keys) {
			tempKeys[j] = n.Keys[i]
			j++
		}
	}
}

func (n *Node) updateInternalWithLeftHalf(tempKeys []types.Key, tempChildren []*Node, splitPoint int) {
	n.Keys = append([]types.Key{}, tempKeys[:splitPoint-1]...)
	n.Children = append([]*Node{}, tempChildren[:splitPoint]...)
}

func (n *Node) createNewInternalNode(tempKeys []types.Key, tempChildren []*Node, splitPoint int) *Node {
	newNode := &Node{IsLeaf: false}
	newNode.Keys = append([]types.Key{}, tempKeys[splitPoint:]...)
	newNode.Children = append([]*Node{}, tempChildren[splitPoint:]...)
	newNode.Parent = n.Parent

	n.updateChildrenParent(newNode)

	return newNode
}

func (n *Node) updateChildrenParent(newNode *Node) {
	for _, child := range newNode.Children {
		if child != nil {
			child.Parent = newNode
		}
	}
}

func (n *Node) InsertIntoNode(leftIndex int, key types.Key, right *Node) error {
	n.shiftKeysAndChildren(leftIndex)
	n.Keys[leftIndex] = key
	n.Children[leftIndex+1] = right
	right.Parent = n
	return nil
}

func (n *Node) shiftKeysAndChildren(leftIndex int) {
	n.Keys = append(n.Keys, "")
	n.Children = append(n.Children, nil)

	for i := len(n.Keys) - 1; i > leftIndex; i-- {
		n.Keys[i] = n.Keys[i-1]
		n.Children[i+1] = n.Children[i]
	}
}

func (n *Node) GetLeftIndex(parent, left *Node) int {
	leftIndex := 0
	for leftIndex <= len(parent.Keys) && parent.Children[leftIndex] != left {
		leftIndex++
	}
	return leftIndex
}
