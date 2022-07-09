package indexer

import (
	"strings"
)

type color bool

const red, black color = true, false

type RBTree struct {
	Root *node
}

type node struct {
	left   *node
	right  *node
	parent *node
	key    string
	// Byte offset from the start of the sorted string table to where the value is stored
	offset    int
	nodecolor color
}

func (n node) string() string {
	return n.key

}

// When writing a entry, in addition to storing it to disk, index the location of the key
func (t *RBTree) Insert(key string, offset int) {

	if t.Root == nil {
		t.Root = &node{
			key:       key,
			offset:    offset,
			nodecolor: black,
		}
		return
	}

	newNode := &node{
		key:       key,
		offset:    offset,
		nodecolor: red,
	}
	n := t.Root
	var parent *node

	loop := true
	for loop {
		parent = n
		switch strings.Compare(key, n.key) {

		case -1:
			if n.left != nil {
				n = n.left
			} else {
				n.left = newNode
				loop = false
			}

			break
		case 0:
			// the key has been updated TODO
			return
		case 1:
			if n.right != nil {
				n = n.right
			} else {
				n.right = newNode
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
func (t *RBTree) validate(n *node) {

	if n == t.Root || n.parent.nodecolor == black {
		return
	}

	x := n
	for x != t.Root && x.parent.nodecolor == red {
		if x.parent == x.parent.parent.left {

			if x.parent.parent.right != nil && x.parent.parent.right.nodecolor == red {
				x.parent.nodecolor = black
				x.parent.parent.nodecolor = red
				x.parent.parent.right.nodecolor = black
				x = x.parent.parent
			} else {

				// case 2
				if x == x.parent.right {
					x = x.parent
					t.rotateleft(x)
				}

				// case 3
				x.parent.nodecolor = black
				x.parent.parent.nodecolor = red
				t.rotateright(x.parent.parent)
			}
		} else {
			y := x.parent.parent.left

			if y.nodecolor == red {
				x.parent.nodecolor = black
				x.parent.parent.nodecolor = red
				y.nodecolor = black
				x = x.parent.parent
			} else {

				if x == x.parent.left {
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

func (t *RBTree) rotateleft(n *node) {

	y := n.right
	n.right = y.left

	if y.left != nil {
		y.left.parent = n
	}

	y.parent = n.parent
	if n.parent == nil {
		t.Root = y
	} else if n == n.parent.left {
		n.parent.left = y
	} else {
		n.parent.right = y
	}

	y.left = n
	n.parent = y

}

func (t *RBTree) rotateright(n *node) {

	y := n.left
	n.left = y.right

	y.parent = n.parent

	if n.parent == nil {
		t.Root = y
	} else if n == n.parent.left {
		n.parent.left = y
	} else {
		n.parent.right = y
	}

	y.right = n
	n.parent = y
}
