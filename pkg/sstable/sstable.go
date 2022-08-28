package sstable

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/crikke/oi/pkg/bloom"
	"github.com/crikke/oi/pkg/memtree"
)

type SSTable struct {
	index   *os.File
	data    *os.File
	summary io.Reader
}

// ErrKeyNotFound if key is not found in sstable
var ErrKeyNotFound = errors.New("key not found in SSTable")

func Get(dir string, key []byte) ([]byte, error) {

	filter, err := bloom.Open(filepath.Join(dir, "bloom.db"))

	if err != nil {
		return nil, err
	}

	if !filter.Exists(key) {
		return nil, ErrKeyNotFound
	}

	summary, err := os.Open(filepath.Join(dir, "summary.db"))
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *SSTable) Close() {
	s.index.Close()
	s.data.Close()
	s.data.Close()
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

	e := indexEntry{
		position:   i.length,
		key:        []byte(n.Key),
		keyLength:  uint16(len(n.Key)),
		dataLength: uint16(l),
	}

	if err = encodeIndexEntry(iw, e); err != nil {
		return err
	}

	// increase size of sstable to get next entry position
	i.length += uint32(l)

	return nil
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

func (s SSTable) scan(offset, length int64, key []byte) (indexEntry, error) {

	if _, err := s.index.Seek(offset, 0); err != nil {
		return indexEntry{}, err
	}

	bytesRead := int64(0)
	for {

		if length != -1 && bytesRead > length {
			break
		}

		e := &indexEntry{}

		n, err := decodeIndexEntry(s.index, e)
		if err != nil {
			if err == io.EOF {
				break
			}
			return indexEntry{}, err
		}

		bytesRead += int64(n)

		if bytes.Equal(e.key, key) {
			return *e, nil
		}
	}

	return indexEntry{}, errors.New("not found")
}
