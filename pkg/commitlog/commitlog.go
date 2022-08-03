package commitlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync"
)

// TODO:
// For now if system failures all SSTables will be discarded and new ones will be remade from the commitlog
// later on use checkpoints instead of discarding all SSTables

// A record holds an mutation which is when state changes (insert, update, delete)
//
// When writing a record to disk, it will calculate the checksum for the mutation
// and get a lsn which is a monotonic number that is used to replay records in the event of failure
type record struct {
	data       []byte
	dataLength uint32
	crc        uint32
	// todo
	// Should LSN start at 0 for each segment?
	// LogSegments are named by incrementing number
	//
	lsn uint32
}

type mutation struct {
	keyLength   uint16
	key         []byte
	valueLength int32
	value       []byte
	tombstone   bool
}

type Writer struct {
	mu      sync.Mutex
	counter int32
	file    os.File
	// size of current segment
	size          int32
	writerChannel chan record
}

func NewWriter(f os.File) *Writer {

	fi, err := f.Stat()

	if err != nil {
		return nil
	}

	// todo
	// bug here if a existing file is opened LSN will reset,
	// could byteoffset be used instead? I think Postgres may do something similar
	w := &Writer{
		mu:            sync.Mutex{},
		writerChannel: make(chan record),
		counter:       0,
		file:          f,
		size:          int32(fi.Size()),
	}

	go w.writeLoop()
	return w
}

func (m mutation) MarshalBinary() ([]byte, error) {

	data := make([]byte, 7)

	binary.LittleEndian.PutUint16(data[0:2], m.keyLength)
	binary.LittleEndian.PutUint32(data[2:6], uint32(m.valueLength))

	tombstone := uint8(0)
	if m.tombstone {
		tombstone = 1
	}
	data[6] = tombstone

	data = append(data, m.key...)
	data = append(data, m.value...)

	return data, nil
}

func makeMutation(key, value []byte, tombstone bool) mutation {

	data := mutation{
		value:       value,
		key:         key,
		keyLength:   uint16(len(key)),
		valueLength: int32(len(value)),
		tombstone:   tombstone,
	}

	return data
}

func (w *Writer) Write(m mutation) error {

	data, err := m.MarshalBinary()
	if err != nil {
		return err
	}

	r := record{
		data:       data,
		crc:        crc32.ChecksumIEEE(data),
		dataLength: uint32(len(data)),
	}

	w.writerChannel <- r
	return nil
}

// flow of an inserting a record
//
// wal.addMutation
// create the mutation and pass it to commitlogLoop which handles setting lsn and appending to file

// todo Check size of segment, currently no check if size is larged than allowed. In the future if size of the segment is larger than a threshold, start appending to a new segment
func (w *Writer) writeLoop() error {

	for {

		r, ok := <-w.writerChannel
		if !ok {
			break
		}

		w.mu.Lock()
		defer w.mu.Unlock()
		r.lsn = uint32(w.counter)
		w.counter++

		data, err := r.MarshalBinary()

		if err != nil {
			return err
		}

		l, err := w.file.Write(data)

		if err != nil {
			return err
		}

		w.size += int32(l)
	}

	return nil
}

func (r record) MarshalBinary() ([]byte, error) {

	data := make([]byte, 12)

	binary.LittleEndian.PutUint32(data[0:4], r.lsn)
	binary.LittleEndian.PutUint32(data[4:8], r.dataLength)

	if r.crc == uint32(0) {
		return nil, errors.New("record missing checksum")
	}

	binary.LittleEndian.PutUint32(data[8:12], r.crc)

	data = append(data, r.data...)
	return data, nil
}
