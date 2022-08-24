package memtree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRBInsert(t *testing.T) {

	type mocknode struct {
		key   string
		color color
	}

	tests := []struct {
		name   string
		keys   [][]byte
		expect []mocknode
	}{
		{
			name: "test insert case 3 left line",
			keys: [][]byte{[]byte("c"), []byte("b"), []byte("a")},
			expect: []mocknode{
				{"b", black},
				{"a", red},
				{"c", red},
			},
		},
		{
			name: "test case 1",
			keys: [][]byte{[]byte("bb"), []byte("aa"), []byte("cc"), []byte("a")},
			expect: []mocknode{
				{"bb", red},
				{"aa", black},
				{"a", red},
				{"cc", black},
			},
		},
		{
			//      cc
			//    aa  nil
			// nil  bb
			//
			//      bb
			//    aa   cc
			name: "test case 2",
			keys: [][]byte{[]byte("cc"), []byte("aa"), []byte("bb")},
			expect: []mocknode{
				{"bb", black},
				{"aa", red},
				{"cc", red},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rbt := &RBTree{}

			for _, k := range test.keys {
				rbt.Insert(k, nil)
			}

			i := 0
			//frisit all nodes from left to right
			traverseTree(
				t,
				func(n *Node, i int) {
					assert.Equal(t, test.expect[i].color, n.nodecolor)
					assert.Equal(t, []byte(test.expect[i].key), n.Key)
				},
				rbt.Root,
				&i,
			)
		})
	}
}

func traverseTree(t *testing.T, assert func(*Node, int), n *Node, idx *int) {

	assert(n, *idx)

	if n.Left != nil {
		*idx++
		traverseTree(t, assert, n.Left, idx)
	}

	if n.Right != nil {
		*idx++
		traverseTree(t, assert, n.Right, idx)
	}
}
