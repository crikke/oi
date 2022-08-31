package sstable

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/crikke/oi/pkg/bloom"
	"github.com/crikke/oi/pkg/memtree"
	pb "github.com/crikke/oi/proto-gen/data"
	"google.golang.org/protobuf/proto"
)

// TODO: currently the data file only stores the value
// the data file should instead store a data struct aswell as its length.
// this would allow data file to work without the index file and creating the index file from the data file.
//
// currently the index and data file are created at the same time.

// TODO: SStables are currently using name for ordering.
// this means that if a sstable is renamed, the order is changed and the data is not valid
// so this needs to be fixed later on


type TableEntry struct {
	r pb.Record
}


// index and summary have pretty much equal data structure

// index depends on data file for position
// summary depends on index for position 
// 
// Creation of new sstable:
// create each file.
// since SStable are immutable create or trunc existing files
// 
// for each file create a chan
// when inserted into data: put data offset and key into indexChannel 
// when inserted into index: put index offset and key into summaryCh

type protoEntry struct {
	data []byte
	dataLen uint32
}

func (p protoEntry) MarshalBinary() ([]byte, error) {

	buf := make([]byte, p.dataLen + 4)
	binary.LittleEndian.PutUint32(buf[0:4], p.dataLen)
	buf = append(buf[4:], p.data...)

	return buf, nil
}

type SSTable struct {
	dir string
	
	entries int 
	data appendOnlyFile
	index appendOnlyFile
	summary appendOnlyFile
	done chan struct {}
	sampleSize int
}

type appendOnlyFile struct {
	w bufio.Writer
	f *os.File
	writerCh chan protoEntry
	done chan struct{}
	size uint32
}

func newAppendOnlyFile(path string, done chan struct{}) *appendOnlyFile {

	aof := &appendOnlyFile{
		done: done,
	}

	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	aof.f = f
	aof.w = bufio.NewWriter(f)
}

func (a* appendOnlyFile) append(p protoEntry) error {

	a.writerCh <- p
	return nil 
}

func (a *appendOnlyFile) writerLoop() {

	for {
		select
		{
		case <- a.done:
			a.w.Flush()
			a.f.Close()
			return
		case p := <-a.writerCh: 
			b, _ := p.MarshalBinary()
			n, err := a.w.Write(b)
			if err != nil {
				panic(err)
			}

			a.size += n
		}
	}
}

func NewSSTable(dir string) *SSTable {

	s := *&SSTable{
		done: make(chan struct{}),
	}
	s.data = *newAppendOnlyFile(filepath.Join(dir, "data.db"), s.done)	
	s.index = *newAppendOnlyFile(filepath.Join(dir, "index.db"), s.done)	
	s.summary = *newAppendOnlyFile(filepath.Join(dir, "summary.db"), s.done)	
}

func (s *SSTable) Append(r pb.Record) error{
	data, err := proto.Marshal(r)

	if err != nil {
		return err
	}

	p := protoEntry{
		data:data,
		dataLen: len(data),
	}
	
	pos := s.data.size
	if err := s.data.append(p); err != nil {
		return err
	}
	
	indexEntry := pb.IndexEntry{
		Key:r.Key,
		Position: pos,
	}
	
	
	data, err := proto.Marshal(&indexEntry)
	if err != nil {
		return err
	}

	p = protoEntry{
		data:data,
		dataLen: len(data),
	}

	if err := s.index.append(p); err != nil {
		return err
	}
	
	if s.entries % s.sampleSize == 0 {
		pos = s.index.size

		summaryEntry := pb.IndexEntry{
			Key:r.Key,
			Position: pos,
		}

		data, err := proto.Marshal(&summaryEntry)
		if err != nil {
			return err
		}

		p = protoEntry{
			data:data,
			dataLen: len(data),
		}

		if err := s.summary.append(p); err != nil {
			return err
		}
	

	}

	s.entries++
}

func (s *SSTable) Done() error {

	s.done <- struct{}{}
	return nil
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

	// create data file
	df, err := newDataFile(filepath.Join(sstableDir, "data.db"))
	if err != nil {
		return err
	}

	// append data to data file 
	if err := df.appendRBtree(m); err != nil {
		return err
	}


	if err := df.Close(); err != nil {
		return err
	}

	// create index file from data file
	if


	return nil
}

// traverses the memtree and wrties the data to the files

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
