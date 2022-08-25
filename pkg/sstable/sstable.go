package sstable

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/crikke/oi/pkg/memtree"
)

type SSTable struct {
	index   *os.File
	data    *os.File
	summary io.Reader
}

// opens a SSTable for reading
// path is where sstable is stored. file ending must be omitted
func Open(path string) (s *SSTable, err error) {

	s = &SSTable{}

	data, err := os.Open(fmt.Sprintf("%s.data", path))

	if err != nil {
		return nil, err
	}

	s.data = data

	idx, err := os.Open(fmt.Sprintf("%s.idx", path))

	if err != nil {
		return nil, err
	}

	s.index = idx

	return
}

func (s *SSTable) Close() {
	s.index.Close()
	s.data.Close()
	s.data.Close()
}

// A Sorted string table cosist of an index file (.idx) and the corresponding data (.db)
// The SSTable is immutable and can only be read from.
// TODO: Create Summary file
type index struct {
	entries []entry
	length  uint32
}

// size of each entry should be:
// 16bit (key length) + (key length * 8)  + 32bit(position)
// key could be shorter than 16 bytes
type entry struct {
	key          []byte
	keyLength    uint16
	position     uint32
	entityLength uint16
}

// calculate the checksum for the file, this will be stored somewhere and is used to compare the index & data file
// if the checksum does not match, the SSTable will be rebuilt from the CommitLog
func checksum(r io.Reader) ([]byte, error) {
	hash := md5.New()

	_, err := io.Copy(hash, r)

	if err != nil {
		return nil, err
	}
	return hash.Sum(nil), nil
}

// creates a new SSTable at given path from a RBTree
func New(name string, m memtree.RBTree) error {

	// assert that the files does not exist
	if _, err := os.Stat(fmt.Sprintf("%s.data", name)); !errors.Is(err, os.ErrNotExist) {
		return os.ErrExist
	}

	if _, err := os.Stat(fmt.Sprintf("%s.idx", name)); !errors.Is(err, os.ErrNotExist) {
		return os.ErrExist
	}

	if _, err := os.Stat(fmt.Sprintf("%s.summary", name)); !errors.Is(err, os.ErrNotExist) {
		return os.ErrExist
	}

	data, err := os.Create(fmt.Sprintf("%s.data", name))
	if err != nil {
		return err
	}
	defer data.Close()

	idx, err := os.Create(fmt.Sprintf("%s.idx", name))

	if err != nil {
		return err
	}
	// todo

	//summary, err := os.Create(fmt.Sprintf("%s.summary", name))
	// if err != nil {
	//	return err
	// }

	createSSTable(idx, data, m)

	return nil
}

// traverses the memtree and wrties the data to the files
func createSSTable(iw io.Writer, db io.Writer, m memtree.RBTree) error {

	s := &index{}
	stack := make([]*memtree.Node, 0)

	current := m.Root
	for len(stack) > 0 || current != nil {

		if current != nil {

			stack = append(stack, current)
			current = current.Left
		}

		if current == nil {
			// pop:w

			el := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if err := s.processNode(iw, db, el); err != nil {
				return err
			}
			current = el.Right
		}
	}

	return nil
}

func (i *index) processNode(iw io.Writer, db io.Writer, n *memtree.Node) error {
	l, err := db.Write(n.Value)

	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	e := entry{
		position:     i.length,
		key:          []byte(n.Key),
		keyLength:    uint16(len(n.Key)),
		entityLength: uint16(l),
	}

	if err = encodeIndexEntry(iw, e); err != nil {
		return err
	}

	// increase size of sstable to get next entry position
	i.length += uint32(l)

	return nil
}

func encodeIndexEntry(iw io.Writer, e entry) error {

	kl := make([]byte, 2)
	pos := make([]byte, 4)
	entityLength := make([]byte, 2)

	binary.LittleEndian.PutUint16(kl, e.keyLength)
	binary.LittleEndian.PutUint32(pos, e.position)
	binary.LittleEndian.PutUint16(entityLength, e.entityLength)

	if _, err := iw.Write(kl); err != nil {
		return err
	}

	if _, err := iw.Write(e.key); err != nil {
		return err
	}
	if _, err := iw.Write(pos); err != nil {
		return err
	}

	if _, err := iw.Write(entityLength); err != nil {
		return err
	}

	return nil
}

func decodeEntry(idx io.Reader, e *entry) (int, error) {

	kl := make([]byte, 2)
	if _, err := idx.Read(kl); err != nil {
		return 0, err
	}

	e.keyLength = binary.LittleEndian.Uint16(kl)
	key := make([]byte, e.keyLength)

	keyBytesRead, err := idx.Read(key)
	if err != nil {
		return 0, err
	}

	pos := make([]byte, 4)
	if _, err := idx.Read(pos); err != nil {
		return 0, err
	}

	e.key = key
	e.position = binary.LittleEndian.Uint32(pos)

	return 6 + keyBytesRead, nil
}

func (s SSTable) Get(key []byte) ([]byte, error) {

	length := int64(-1)
	// todo length should be blocksize from summary file
	entry, err := s.scan(0, length, key)

	if err != nil {
		return nil, err
	}

	val := make([]byte, entry.keyLength)
	if _, err := s.data.ReadAt(val, int64(entry.position)); err != nil {
		return nil, err
	}

	return val, nil
}

// searches the index for key starting at offset.
// It will continue search until end or bytes read > length, in which key does not exist
// if length is -1 it will keep scanning until EOF

func (s SSTable) scan(offset, length int64, key []byte) (entry, error) {

	if _, err := s.index.Seek(offset, 0); err != nil {
		return entry{}, err
	}

	bytesRead := int64(0)
	for {

		if length != -1 && bytesRead > length {
			break
		}

		e := &entry{}

		n, err := decodeEntry(s.index, e)
		if err != nil {
			if err == io.EOF {
				break
			}
			return entry{}, err
		}

		bytesRead += int64(n)

		if bytes.Equal(e.key, key) {
			return *e, nil
		}
	}

	return entry{}, errors.New("not found")
}
