package memtree

import (
	"strings"
)

type color bool

const red, black color = true, false

type RBTree struct {
	Root *Node
}

type Node struct {
	Left      *Node
	Right     *Node
	parent    *Node
	Key       string
	Value     []byte
	nodecolor color
}

func (n Node) string() string {
	return n.Key

}

// When writing a entry, in addition to storing it to disk, index the location of the key
func (t *RBTree) Insert(key string, value []byte) {

	if t.Root == nil {
		t.Root = &Node{
			Key:       key,
			nodecolor: black,
			Value:     value,
		}
		return
	}

	newNode := &Node{
		Key:       key,
		Value:     value,
		nodecolor: red,
	}
	n := t.Root
	var parent *Node

	loop := true
	for loop {
		parent = n
		switch strings.Compare(key, n.Key) {

		case -1:
			if n.Left != nil {
				n = n.Left
			} else {
				n.Left = newNode
				loop = false
			}

			break
		case 0:
			// the key has been updated TODO
			return
		case 1:
			if n.Right != nil {
				n = n.Right
			} else {
				n.Right = newNode
				loop = false
			}
			break
		}
	}
	newNode.parent = parent

	t.validate(newNode)
}

// Validation cases
// 1. If uncle is red - switch color on uncle, parent, grandparent
// 2. if uncle is black and path forms a trinagle, rotate opposite direction
// 3. if uncle is black and path forms a line, rotate opposite direction and recolor

// validates the tree
// returns 0 if valid
// returns:
// 1 - uncle is red
// 2 - uncle is black and triangle
// 3 - uncle is black and line
func (t *RBTree) validate(n *Node) {

	if n == t.Root || n.parent.nodecolor == black {
		return
	}

	x := n
	for x != t.Root && x.parent.nodecolor == red {
		if x.parent == x.parent.parent.Left {

			if x.parent.parent.Right != nil && x.parent.parent.Right.nodecolor == red {
				x.parent.nodecolor = black
				x.parent.parent.nodecolor = red
				x.parent.parent.Right.nodecolor = black
				x = x.parent.parent
			} else {

				// case 2
				if x == x.parent.Right {
					x = x.parent
					t.rotateleft(x)
				}

				// case 3
				x.parent.nodecolor = black
				x.parent.parent.nodecolor = red
				t.rotateright(x.parent.parent)
			}
		} else {
			y := x.parent.parent.Left

			if y != nil && y.nodecolor == red {
				x.parent.nodecolor = black
				x.parent.parent.nodecolor = red
				y.nodecolor = black
				x = x.parent.parent
			} else {

				if x == x.parent.Left {
					x = x.parent
					t.rotateright(x)
				}
				x.parent.nodecolor = black
				x.parent.parent.nodecolor = red
				t.rotateleft(x.parent.parent)

			}

		}
	}
}

func (t *RBTree) rotateleft(n *Node) {

	y := n.Right
	n.Right = y.Left

	if y.Left != nil {
		y.Left.parent = n
	}

	y.parent = n.parent
	if n.parent == nil {
		t.Root = y
	} else if n == n.parent.Left {
		n.parent.Left = y
	} else {
		n.parent.Right = y
	}

	y.Left = n
	n.parent = y

}

func (t *RBTree) rotateright(n *Node) {

	y := n.Left
	n.Left = y.Right

	y.parent = n.parent

	if n.parent == nil {
		t.Root = y
	} else if n == n.parent.Left {
		n.parent.Left = y
	} else {
		n.parent.Right = y
	}

	y.Right = n
	n.parent = y
}
