package commitlog

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
)

type Writer struct {
	mu            sync.Mutex
	counter       uint32
	file          *os.File
	segmentNumber uint32

	// size of current segment
	size           int32
	writerChannel  chan Record
	logDir         string
	maxSegmentSize int
}

func NewWriter(ctx context.Context, logDir string, maxSegmentSize int) (*Writer, error) {

	f, err := GetLatestSegment(logDir, maxSegmentSize)

	if err != nil {
		return nil, fmt.Errorf("[New Writer] fatal: %w", err)
	}

	fi, err := f.Stat()

	if err != nil {
		return nil, fmt.Errorf("[New Writer] fatal: %w", err)
	}

	segmentNumber, err := parseSegmentName(f.Name())
	records, err := ReadLogSegment(ctx, f)
	if err != nil {
		return nil, fmt.Errorf("[New Writer] fatal: %w", err)
	}

	w := &Writer{
		mu:             sync.Mutex{},
		writerChannel:  make(chan Record),
		file:           f,
		size:           int32(fi.Size()),
		logDir:         logDir,
		counter:        uint32(len(records)) - 1,
		maxSegmentSize: maxSegmentSize,
		segmentNumber:  uint32(segmentNumber),
	}

	go w.writeLoop(ctx)
	return w, nil
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

func (w *Writer) writeLoop(ctx context.Context) error {

	for {
		select {

		case r := <-w.writerChannel:

			w.mu.Lock()
			defer w.mu.Unlock()

			// TODO: !! IMPORTANT THIS MUST BE MOVED IF NEW SEGMENT IS CREATED
			r.LSN = uint64(w.segmentNumber + w.counter)
			w.counter++

			data, err := r.MarshalBinary()
			if err != nil {
				return fmt.Errorf("[writeLoop] error: %w", err)
			}

			if len(data)+int(w.size) > w.maxSegmentSize {
				if err := w.nextSegment(); err != nil {
					return fmt.Errorf("[writeLoop] fatal: %w", err)
				}
			}

			l, err := w.file.Write(data)

			if err != nil {
				return fmt.Errorf("[writeLoop] fatal: %w", err)
			}

			w.size += int32(l)

		case <-ctx.Done():
			break

		}
	}
}

// closes the current segmentfile and creates the next segment
func (w *Writer) nextSegment() error {

	w.file.Close()
	w.segmentNumber += 1
	name := fmt.Sprintf("%s%d%s", LogPrefix, w.segmentNumber, LogSuffix)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND, 660)
	if err != nil {
		return fmt.Errorf("[nextSegment] internal error: %w", err)
	}
	w.counter = 0
	w.file = f

	return nil
}
