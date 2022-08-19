package commitlog

import (
	"errors"
	"hash/crc32"
	"os"
	"sync"
)

var ErrMaxSegmentSizeReached = errors.New("max segment size reached")

type Writer struct {
	mu            sync.Mutex
	counter       uint32
	file          *os.File
	segmentNumber uint32

	// size of current segment
	size           int32
	writerChannel  chan Record
	done           chan error
	logDir         string
	maxSegmentSize int
}

func NewWriter(logDir string, maxSegmentSize int) *Writer {

	f, err := GetCurrentSegment(logDir, maxSegmentSize)

	if err != nil {
		return nil
	}

	fi, err := f.Stat()

	if err != nil {
		return nil
	}

	segmentNumber, err := parseSegmentName(f.Name())
	records := ReadLogSegment(f)
	if err != nil {
		panic(err)
	}

	// todo
	// bug here if a existing file is opened LSN will reset,
	// could byteoffset be used instead? I think Postgres may do something similar
	w := &Writer{
		mu:             sync.Mutex{},
		writerChannel:  make(chan Record),
		done:           make(chan error),
		file:           f,
		size:           int32(fi.Size()),
		logDir:         logDir,
		counter:        uint32(len(records)) - 1,
		maxSegmentSize: maxSegmentSize,
		segmentNumber:  uint32(segmentNumber),
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
		Data:       data,
		Crc:        crc32.ChecksumIEEE(data),
		DataLength: uint32(len(data)),
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
		select {

		case r := <-w.writerChannel:

			w.mu.Lock()
			defer w.mu.Unlock()

			r.LSN = uint64(w.segmentNumber + w.counter)
			w.counter++

			// TODO: inc record only
			data, err := r.MarshalBinary()

			if len(data)+int(w.size) > w.maxSegmentSize {
			}

			if err != nil {
				return err
			}

			l, err := w.file.Write(data)

			if err != nil {
				return err
			}

			w.size += int32(l)

		case <-w.done:
			break

		}
	}

	return nil
}

func (w *Writer) Close() {

}

func (w *Writer) nextSegment() {

	w.file.Close()
	w.segmentNumber +=1
	name := fmt.Sprintf("%s%d%s", LogPrefix, w.segmentNumber, LogSuffix)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND, 660)
	if err != nil {
		panic(err)
	}
	w.counter = 0
	w.file = f
