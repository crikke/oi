package memtree

import "errors"

type Configuration struct {

	// Size in bytes before memtree is written to disk and flushed
	// defaults to 64kb
	MaxSize int
}

var ErrMaxSizeReached = errors.New("maximum size reached")

// A memory tree stores the lasted writes in memory.
// Once the tree is full, the data is written to disk as a SSTable and the tree is flushed

type Memtree struct {
	Size    int
	MaxSize int

	rbt RBTree
}

// Initalizes a memtree for a database,
func NewMemtree(maxSize int) *Memtree {

	return &Memtree{
		Size:    0,
		MaxSize: maxSize,
		rbt:     RBTree{},
	}
}

// todo: calculate size in Kb of rbtree needs to be looked at
// for now checking key + value size and storing it is suffient, but should look if there is a better way to do it later.
func (m *Memtree) Put(key, value []byte) error {

	if m.Size+len(value)+len(key) > m.MaxSize {
		return ErrMaxSizeReached
	}

	m.Size += len(key) + len(value)
	m.rbt.Insert(key, value)

	return nil
}

func (m Memtree) Get(key []byte) ([]byte, bool) {
	return nil, false
}
