package sstable

import (
	"encoding/binary"
	"io"

	"github.com/crikke/oi/pkg/memtree"
)

// A Sorted string table cosist of an index file (.idx) and the corresponding data (.db)
// The SSTable is immutable and can only be read from.
// TODO: Create Summary file
type sstable struct {
	entries []entry
	length  uint32
}

// size of each entry should be:
// 16bit (key length) + (key length * 8)  + 32bit(position)
// key could be shorter than 16 bytes
type entry struct {
	key       []byte
	keyLength uint16
	position  uint32
}

func createIndex(iw io.Writer, db io.Writer, m memtree.RBTree) {

	s := &sstable{}
	stack := make([]*memtree.Node, 0)

	current := m.Root
	for len(stack) > 0 || current != nil {
		stack = append(stack, current)
		current = current.Left

		if current == nil {
			// pop:w

			el := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			s.processNode(iw, db, el)
			current = el.Right
		}
	}
}

func (s *sstable) processNode(iw io.Writer, db io.Writer, n *memtree.Node) error {
	l, err := db.Write(n.Value)
	if err != nil {
		return err
	}
	// write \x00 NULL to mark end of data
	_, err = db.Write([]byte("\x00"))

	if err != nil {
		return err
	}

	e := entry{
		position:  s.length,
		key:       []byte(n.Key),
		keyLength: uint16(len(n.Key)),
	}

	if err = encodeEntry(iw, e); err != nil {
		return err
	}

	// increase size of sstable to get next entry position, add 1 extra byte for the null escape character
	s.length += uint32(l + 1)

	return nil
}

func encodeEntry(iw io.Writer, e entry) error {

	kl := make([]byte, 2)
	pos := make([]byte, 4)

	binary.LittleEndian.PutUint16(kl, e.keyLength)
	binary.LittleEndian.PutUint32(pos, e.position)

	iw.Write(kl)
	iw.Write(e.key)
	iw.Write(pos)

	return nil
}