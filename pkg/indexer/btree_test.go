package indexer

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
		keys   []string
		expect []mocknode
	}{
		{
			name: "test insert case 3 left line",
			keys: []string{"c", "b", "a"},
			expect: []mocknode{
				{"b", black},
				{"a", red},
				{"c", red},
			},
		},
		{
			name: "test case 1",
			keys: []string{"bb", "aa", "cc", "a"},
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
			//      aa
			//    bb   cc
			name: "test case 2",
			keys: []string{"cc", "aa", "bb"},
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
				rbt.Insert(k, 0)
			}

			i := 0
			//frisit all nodes from left to right
			traverseTree(
				t,
				func(n *node, i int) {
					assert.Equal(t, test.expect[i].color, n.nodecolor)
					assert.Equal(t, test.expect[i].key, n.key)
				},
				rbt.Root,
				&i,
			)
		})
	}
}

func traverseTree(t *testing.T, assert func(*node, int), n *node, idx *int) {

	assert(n, *idx)

	if n.left != nil {
		*idx++
		traverseTree(t, assert, n.left, idx)
	}

	if n.right != nil {
		*idx++
		traverseTree(t, assert, n.right, idx)
	}
}
