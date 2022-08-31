package commitlog

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"sync"

	pb "github.com/crikke/oi/proto-gen/data"

	"google.golang.org/protobuf/proto"
)

type Writer struct {
	mu            sync.Mutex
	counter       uint32
	file          *os.File
	segmentNumber uint32

	// size of current segment
	size           int32
	writerChannel  chan *pb.Mutation
	logDir         string
	maxSegmentSize int
	// CallbackFn is called after the writeloop has successfully written the record.
	// This is used to insert the mutation into the memtree
	callbackFn func(m *pb.Mutation) error
}

func NewWriter(ctx context.Context, logDir string, maxSegmentSize int, callbackFn func(m *pb.Mutation) error) (*Writer, error) {

	f, err := GetLatestSegment(logDir, maxSegmentSize)

	if err != nil {
		return nil, fmt.Errorf("[New Writer] fatal: %w", err)
	}

	fi, err := f.Stat()

	if err != nil {
		return nil, fmt.Errorf("[New Writer] fatal: %w", err)
	}

	segmentNumber, err := parseSegmentName(f.Name())
	if err != nil {
		return nil, fmt.Errorf("[New Writer] fatal: %w", err)
	}

	records, err := ReadLogSegment(ctx, f)
	if err != nil {
		return nil, fmt.Errorf("[New Writer] fatal: %w", err)
	}

	w := &Writer{
		mu:             sync.Mutex{},
		writerChannel:  make(chan *pb.Mutation),
		file:           f,
		size:           int32(fi.Size()),
		logDir:         logDir,
		counter:        uint32(len(records)) - 1,
		maxSegmentSize: maxSegmentSize,
		segmentNumber:  uint32(segmentNumber),
		callbackFn:     callbackFn,
	}

	go w.writeLoop(ctx)
	return w, nil
}

func (w *Writer) Write(m *pb.Mutation) error {

	w.writerChannel <- m
	return nil
}

func (w *Writer) writeLoop(ctx context.Context) error {

	for {
		select {

		case m := <-w.writerChannel:

			data, err := proto.Marshal(m)
			if err != nil {
				return err
			}

			r := pb.Record{
				Data:     m,
				Checksum: crc32.ChecksumIEEE(data),
			}

			w.mu.Lock()
			defer w.mu.Unlock()

			// TODO: !! IMPORTANT THIS MUST BE MOVED IF NEW SEGMENT IS CREATED
			r.LSN = uint64(w.segmentNumber + w.counter)
			w.counter++

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

			if err := w.callbackFn(m); err != nil {
				return err
			}

		case <-ctx.Done():
			return nil

		}
	}
}

// closes the current segmentfile and creates the next segment
func (w *Writer) nextSegment() error {

	w.file.Close()
	w.segmentNumber += 1
	name := fmt.Sprintf("%s%d%s", LogPrefix, w.segmentNumber, LogSuffix)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		return fmt.Errorf("[nextSegment] internal error: %w", err)
	}
	w.counter = 0
	w.file = f

	return nil
}
