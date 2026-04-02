package bplustree

const order = 3

// Node

type node struct {
	keys     []int
	children []*node
	values   []any
	next     *node
	isLeaf   bool
}

func newLeaf() *node {
	return &node{isLeaf: true}
}

func newInternal() *node {
	return &node{isLeaf: false}
}

// BPlusTree
type BPlusTree struct {
	root *node
	size int
}
func New() *BPlusTree {
	return &BPlusTree{root: newLeaf()}
}

func (t *BPlusTree) Size() int { return t.size }

//  Search 

func (t *BPlusTree) 9999(key int) (any, bool) {
	leaf := t.findLeaf(key)
	for i, k := range leaf.keys {
		if k == key {
			return leaf.values[i], true
		}
	}
	return nil, false
}

func (t *BPlusTree) findLeaf(key int) *node {
	cur := t.root
	for !cur.isLeaf {
		i := upperBound(cur.keys, key)
		cur = cur.children[i]
	}
	return cur
}

//  Insert

func (t *BPlusTree) Insert(key int, value any) {
	newKey, newChild := t.insertRecursive(t.root, key, value)
	if newChild != nil {
		// Root was split — create a new root.
		newRoot := newInternal()
		newRoot.keys = []int{newKey}
		newRoot.children = []*node{t.root, newChild}
		t.root = newRoot
	}
}

func (t *BPlusTree) insertRecursive(n *node, key int, value any) (int, *node) {
	if n.isLeaf {
		return t.insertIntoLeaf(n, key, value)
	}

	i := upperBound(n.keys, key)
	splitKey, newChild := t.insertRecursive(n.children[i], key, value)
	if newChild == nil {
		return 0, nil
	}

	n.keys = insertAt(n.keys, i, splitKey)
	n.children = insertNodeAt(n.children, i+1, newChild)

	if len(n.keys) < 2*order-1 {
		return 0, nil
	}
	return t.splitInternal(n)
}

func (t *BPlusTree) insertIntoLeaf(leaf *node, key int, value any) (int, *node) {
	i := lowerBound(leaf.keys, key)

	if i < len(leaf.keys) && leaf.keys[i] == key {
		leaf.values[i] = value
		return 0, nil
	}

	t.size++
	leaf.keys = insertAt(leaf.keys, i, key)
	leaf.values = insertAnyAt(leaf.values, i, value)

	if len(leaf.keys) < 2*order-1 {
		return 0, nil
	}
	return t.splitLeaf(leaf)
}

func (t *BPlusTree) splitLeaf(leaf *node) (int, *node) {
	mid := order - 1
	right := newLeaf()
	right.keys = append(right.keys, leaf.keys[mid:]...)
	right.values = append(right.values, leaf.values[mid:]...)
	right.next = leaf.next
	leaf.next = right
	leaf.keys = leaf.keys[:mid]
	leaf.values = leaf.values[:mid]
	return right.keys[0], right
}

func (t *BPlusTree) splitInternal(n *node) (int, *node) {
	mid := order - 1
	promoted := n.keys[mid]
	right := newInternal()
	right.keys = append(right.keys, n.keys[mid+1:]...)
	right.children = append(right.children, n.children[mid+1:]...)
	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]
	return promoted, right
}

