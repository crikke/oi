package sstable

import (
	"crypto/md5"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/crikke/oi/pkg/bloom"
	"github.com/crikke/oi/pkg/memtree"
)

// TODO: currently the data file only stores the value
// the data file should instead store a data struct aswell as its length.
// this would allow data file to work without the index file and creating the index file from the data file.
//
// currently the index and data file are created at the same time.

// TODO: SStables are currently using name for ordering.
// this means that if a sstable is renamed, the order is changed and the data is not valid
// so this needs to be fixed later on

type SSTable struct {
	index   *os.File
	data    *os.File
	summary io.Reader
}

// ErrKeyNotFound if key is not found in sstable
var ErrKeyNotFound = errors.New("key not found in SSTable")

// Get value.
// When searching for key, it will search each sstable ordered from the most recent to oldest until key is found
func Get(dataDir string, key []byte) ([]byte, error) {

	dirEntries, err := os.ReadDir(dataDir)

	if err != nil {
		return nil, err
	}
	for i := len(dirEntries) - 1; i >= 0; i-- {

		entry := dirEntries[i]

		if !entry.IsDir() {
			continue
		}

		value, err := getFromSStable(entry.Name(), key)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		if value != nil {
			return value, nil
		}

	}
	return nil, nil
}

// TODO: handle checksum check
func getFromSStable(dir string, key []byte) ([]byte, error) {

	filter, err := bloom.Open(filepath.Join(dir, "bloom.db"))

	if err != nil {
		return nil, err
	}

	if !filter.Exists(key) {
		return nil, ErrKeyNotFound
	}

	summary, err := os.Open(filepath.Join(dir, "summary.db"))
	defer summary.Close()
	if err != nil {
		return nil, err
	}

	se, err := getSummaryEntry(summary, key)

	if err != nil {
		return nil, err
	}

	ie, err := getIndexEntry(filepath.Join(dir, "index.db"), key, se.position)

	if err != nil {
		return nil, err
	}

	data, err := os.Open(filepath.Join(dir, "data.db"))
	defer data.Close()
	if err != nil {
		return nil, err
	}

	entry, err := getDataEntry(filepath.Join(dir, "data.db"), ie.position)

	if err != nil {
		return nil, err
	}

	// TODO: compare checksum of data entry
	if entry.Header.DeletionTime.IsZero() {
		return nil, ErrKeyNotFound
	}

	return entry.value, nil
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
// The creation logic work by first creating the data file followed by the index file and then the summary file
func New(dataDir string, m memtree.RBTree) error {

	dirEntries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}

	sstableDir := filepath.Join(dataDir, string(len(dirEntries)))

	err = os.Mkdir(sstableDir, 0660)
	if err != nil {
		return err
	}

	df, err := newDataFile(filepath.Join(sstableDir, "data.db"))
	if err != nil {
		return err
	}

	traverseRBTree(m, func(n *memtree.Node) error {

		de := DataEntry{
			key:   n.Key,
			value: n.Value,
			Header: &DataEntryHeader{
				ValueLength: uint16(len(n.Value)),
				KeyLength:   uint16(len(n.Key)),
			},
		}

		if err := df.append(de); err != nil {
			return err
		}
		return nil
	})

	if err := df.Close(); err != nil {
		return err
	}
	return nil
}

// traverses the memtree and wrties the data to the files
func traverseRBTree(m memtree.RBTree, callback func(n *memtree.Node) error) error {

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

			if err := callback(el); err != nil {
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
