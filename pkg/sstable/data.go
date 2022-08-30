package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"time"

	"github.com/crikke/oi/pkg/memtree"
)

type dataFile struct {
	w *bufio.Writer
	f *os.File
}

func (df *dataFile) Close() error {

	if err := df.w.Flush(); err != nil {
		return err
	}

	return df.f.Close()
}

func newDataFile(path string) (*dataFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0660)

	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(f)

	df := &dataFile{
		f: f,
		w: w,
	}
	return df, nil
}

func (df *dataFile) appendRBtree(m memtree.RBTree) error {

	stack := make([]*memtree.Node, 0)

	current := m.Root
	for len(stack) > 0 || current != nil {

		if current != nil {

			stack = append(stack, current)
			current = current.Left
		}

		if current == nil {

			el := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			de := DataEntry{
				key:   el.Key,
				value: el.Value,
				Header: &DataEntryHeader{
					ValueLength: uint16(len(el.Value)),
					KeyLength:   uint16(len(el.Key)),
				},
			}

			if err := de.writeTo(df.w); err != nil {
				return err
			}
			current = el.Right
		}
	}

	return nil
}

// for now only key & value is allowed. Maybe in the future cells (row & columns) could be used.
type DataEntry struct {
	Header *DataEntryHeader
	key    []byte
	value  []byte
}

type DataEntryHeader struct {
	DeletionTime time.Time
	ValueLength  uint16
	KeyLength    uint16
}

func (de DataEntry) writeTo(w io.Writer) error {

	if err := binary.Write(w, binary.LittleEndian, de.Header); err != nil {
		return err
	}

	if _, err := w.Write(de.key); err != nil {
		return err
	}

	if _, err := w.Write(de.value); err != nil {
		return err
	}

	return nil
}

func (de *DataEntry) readFrom(r io.Reader) error {

	if err := binary.Read(r, binary.LittleEndian, de.Header); err != nil {
		return err
	}

	de.key = make([]byte, de.Header.KeyLength)
	de.value = make([]byte, de.Header.ValueLength)

	if _, err := r.Read(de.key); err != nil {
		return err
	}

	if _, err := r.Read(de.value); err != nil {
		return err
	}
	return nil
}

func getDataEntry(file string, pos int64) (*DataEntry, error) {
	f, err := os.Open(file)

	if err != nil {
		return nil, err
	}

	_, err = f.Seek(pos, 0)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReader(f)

	de := &DataEntry{}

	if err := de.readFrom(r); err != nil {
		return nil, err
	}

	return de, nil
}
