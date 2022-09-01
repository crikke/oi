package commitlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/crikke/oi/pkg/data"
	pb "github.com/crikke/oi/proto-gen/data"
	"github.com/golang/protobuf/proto"
)

const (
	LogPrefix = "log_"
	LogSuffix = ".log"
)

// TODO: Ensure that each record is pure
// A mutation must contain the actual value and not for example time.Now or previous + 1
// This must be enforced in order to guarantee that the records will be replayed correctly and not
// corrupt state.

func ReadLogSegment(ctx context.Context, r io.Reader) ([]*pb.Record, error) {
	records := make([]*pb.Record, 0)
	loop := true
	for loop {
		select {
		case <-ctx.Done():
			break
		default:

			pe := &data.ProtoEntry{}
			if _, err := pe.ReadFrom(r); err != nil {

				if errors.Is(err, io.EOF) {
					loop = false
					break
				}
				return nil, fmt.Errorf("[ReadLogSegment] fatal: %w", err)
			}
			record := &pb.Record{}

			if err := proto.Unmarshal(pe.Data, record); err != nil {
				return nil, err
			}

			records = append(records, record)

		}

	}

	return records, nil
}

func parseSegmentName(str string) (uint64, error) {

	name := strings.TrimPrefix(strings.TrimSuffix(str, LogSuffix), LogPrefix)
	n, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return 0, err
	}

	return n >> 32, nil

}

// Returns the segmentnumber for the LSN which are the 32 first bits
func SegmentNumber(lsn uint64) uint32 {

	return uint32(lsn >> 32)
}

// returns the recordnumber for the LSN which are the last 32 bits
func RecordNumber(lsn uint64) int {
	return int(lsn & 0xffffffff)
}

// returns the segment that includes the specified lsn and all trailing segments
func GetTrailingSegments(dir string, lsn uint64) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)

	if err != nil {
		return nil, fmt.Errorf("[GetSegmentFiles] fatal: %w", err)
	}

	res := make([]os.DirEntry, 0)

	// remove first 32 bits to get the segmentLsn for the record
	recordLsn := lsn >> 32

	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), LogPrefix) || !strings.HasSuffix(entry.Name(), LogSuffix) {
			continue
		}

		segmentLsn, err := parseSegmentName(entry.Name())

		if err != nil {
			return nil, fmt.Errorf("[GetTrailingSegments] error parsing segment name: %w", err)
		}
		if segmentLsn >= recordLsn {
			res = append(res, entry)
		}
	}

	return res, nil
}

func GetLatestSegment(logDir string, maxSegmentSize int) (*os.File, error) {

	segments, err := GetTrailingSegments(logDir, uint64(0))

	if err != nil {
		return nil, err
	}

	segment := segments[len(segments)-1]

	return os.Open(segment.Name())
}
