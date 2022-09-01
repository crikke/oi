package lsmtree

import (
	"errors"
	"unsafe"

	"github.com/crikke/oi/pkg/data/lsmtree/memtree"
	pb "github.com/crikke/oi/proto-gen/data"
)

type Configuration struct {
	DataDir        string
	MemtreeMaxSize uint32
}

type LSMTree struct {
	appendCh      chan *pb.Mutation
	memTree       *memtree.RBTree
	memTreeSize   uint64
	Configuration *Configuration
}

func NewLSMTree(cfg *Configuration) *LSMTree {
	t := &LSMTree{Configuration: cfg}
	t.appendCh = make(chan *pb.Mutation)
	go t.appendLoop()
	return t
}

func (l *LSMTree) Append(data *pb.Mutation) error {
	l.appendCh <- data
	return nil
}

func (l LSMTree) Get(key []byte) ([]byte, error) {

	val, err := l.Get(key)
	if err != nil {

		if !errors.Is(ErrKeyNotFound, err) {
			return nil, err
		}

		// get from sstable.
	}

	return val, nil
}

func (l *LSMTree) appendLoop() {

	for {
		data := <-l.appendCh

		l.checkIfNeedsFlush(data)
		l.memTree.Insert(data)
	}
}

func (l *LSMTree) checkIfNeedsFlush(data *pb.Mutation) {

	tt := data.GetTombstone().DeletionTime.AsTime()

	size := uint64(len(data.Value)) + uint64(len(data.Key)) + uint64(unsafe.Sizeof(tt))

	if l.memTreeSize+size >= uint64(l.Configuration.MemtreeMaxSize) {

		rbt := *l.memTree
		l.memTree = &memtree.RBTree{}
		l.memTreeSize = 0
		go l.flush(rbt)
	}
}

func (l *LSMTree) flush(rbt memtree.RBTree) error {

	sst := NewSSTable(l.Configuration.DataDir)
	defer sst.Done()
	stack := make([]*memtree.Node, 0)

	current := rbt.Root
	for len(stack) > 0 || current != nil {

		if current != nil {

			stack = append(stack, current)
			current = current.Left
		}

		if current == nil {

			el := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if err := sst.Append(el.Data); err != nil {
				return err
			}
			current = el.Right
		}
	}

	return nil
}
