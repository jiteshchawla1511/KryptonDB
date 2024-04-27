package lsmtree

import "fmt"

type Node struct {
	Data  KV
	Left  *Node
	Right *Node
	Size  int
}

func NewTree(elements []KV) *Node {
	size := len(elements)
	if size == 0 {
		return nil
	}
	if size == 1 {
		return &Node{Data: elements[0]}
	}
	root := &Node{
		Data:  elements[size/2],
		Left:  NewTree(elements[0 : size/2]),
		Right: NewTree(elements[(size/2)+1:]),
		Size:  size,
	}
	return root
}

func Insert(tree **Node, data KV) {
	if *tree == nil {
		*tree = &Node{Data: data, Size: 1}
	} else if data.Key < (*tree).Data.Key {
		Insert(&((*tree).Left), data)
		(*tree).Size++
	} else if data.Key > (*tree).Data.Key {
		Insert(&((*tree).Right), data)
		(*tree).Size++
	} else {
		(*tree).Data.Value = data.Value
		(*tree).Data.Tombstone = false
	}
}

func Delete(tree **Node, key string) {
	if tree == nil {
		return
	} else if key < (*tree).Data.Key {
		Delete(&((*tree).Left), key)
		(*tree).Size++
	} else if key > (*tree).Data.Key {
		Delete(&((*tree).Right), key)
		(*tree).Size++
	} else {
		(*tree).Data.Tombstone = true
	}
}

func (tree *Node) Find(key string) (KV, error) {
	if tree == nil {
		return KV{}, fmt.Errorf("key %s is not found", key)
	}

	if tree.Data.Key == key {
		return tree.Data, nil
	} else if key <= tree.Data.Key {
		return tree.Left.Find(key)
	} else {
		return tree.Right.Find(key)
	}
}

func (tree *Node) All() []KV {
	if tree == nil {
		return []KV{}
	}

	return append(append(tree.Left.All(), tree.Data), tree.Right.All()...)
}

func (tree *Node) JustSmallerOrEqual(key string) (KV, error) {
	if tree == nil {
		return KV{}, fmt.Errorf("key %s is smaller than any key in the tree", key)
	}
	current := tree.Data

	if current.Key == key {
		return current, nil
	}

	if current.Key > key {
		left, err := tree.Left.JustSmallerOrEqual(key)
		if err != nil {
			return KV{}, err
		}
		current = left
	} else {

		right, err := tree.Right.JustSmallerOrEqual(key)
		if err == nil && current.Key < right.Key {
			current = right
		}
	}
	return current, nil
}

func (tree *Node) JustGreater(key string) (KV, error) {
	if tree == nil {
		return KV{}, fmt.Errorf("key %s is larger than any key in the tree", key)
	}
	current := tree.Data

	if current.Key <= key {
		right, err := tree.Right.JustGreater(key)
		if err != nil {
			return KV{}, err
		}
		current = right

	} else {
		left, err := tree.Left.JustGreater(key)
		if err == nil && current.Key > left.Key {
			current = left
		}

	}
	return current, nil
}

func (tree *Node) GetSize() int {
	return tree.Size
}
