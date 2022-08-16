package commitlog

import (
	"hash/crc32"
	"os"
	"sync"
)

// TODO:
// For now if system failures all SSTables will be discarded and new ones will be remade from the commitlog
// later on use checkpoints instead of discarding all SSTables

// A Record holds an mutation which is when state changes (insert, update, delete)
//
// When writing a Record to disk, it will calculate the checksum for the mutation
// and get a lsn which is a monotonic number that is used to replay records in the event of failure
type Writer struct {
	mu      sync.Mutex
	counter int32
	file    os.File
	// size of current segment
	size          int32
	writerChannel chan Record
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
		writerChannel: make(chan Record),
		counter:       0,
		file:          f,
		size:          int32(fi.Size()),
	}

	go w.writeLoop()
	return w
}

func (w *Writer) Write(m Mutation) error {

	data, err := m.MarshalBinary()
	if err != nil {
		return err
	}

	r := Record{
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
		r.LSN = uint64(w.counter)
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
