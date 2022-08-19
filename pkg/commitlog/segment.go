package commitlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const (
	LogPrefix = "log_"
	LogSuffix = ".log"
)

// TODO: Ensure that each record is pure
// A mutation must contain the actual value and not for example time.Now or previous + 1
// This must be enforced in order to guarantee that the records will be replayed correctly and not
// corrupt state.

func ReadLogSegment(f io.Reader) []Record {

	records := make([]Record, 0)
	for {

		r := Record{}

		lsn := make([]byte, 4)
		if _, err := f.Read(lsn); err != nil {

			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}

		r.LSN = binary.LittleEndian.Uint64(lsn)

		dataLen := make([]byte, 4)
		if _, err := f.Read(dataLen); err != nil {
			panic(err)
		}

		r.DataLength = binary.LittleEndian.Uint32(dataLen)

		crc := make([]byte, 4)
		if _, err := f.Read(crc); err != nil {
			panic(err)
		}

		r.Crc = binary.LittleEndian.Uint32(crc)

		data := make([]byte, r.DataLength)

		if _, err := f.Read(data); err != nil {
			panic(err)
		}

		r.Data = data

		records = append(records, r)
	}
	return records
}

func parseSegmentName(str string) (uint64, error) {

	name := strings.TrimPrefix(strings.TrimSuffix(str, LogSuffix), LogPrefix)
	n, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return 0, err
	}

	return n >> 32, nil

}
func NextSegment(logDir string) (*os.File, error) {
	segments, err := GetSegmentFiles(logDir)

	if err != nil {
		return nil, err
	}

	current := segments[len(segments)-1]

	currentName := strings.TrimPrefix(strings.TrimSuffix(current.Name(), LogSuffix), LogPrefix)
	n, err := strconv.ParseUint(currentName, 10, 64)
	if err != nil {
		return nil, err
	}

	next := ((n >> 32) + 1) << 32

	return os.OpenFile(fmt.Sprintf("%s%d%s", LogPrefix, next, LogSuffix), os.O_CREATE|os.O_APPEND, 660)
}

func GetLastAppliedSegment(lsn uint64) uint32 {

	return uint32(lsn >> 32)
}

func GetLastAppliedRecord(lsn uint64) int {
	return int(lsn & 0xffffffff)
}

func GetSegmentFiles(dir string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)

	if err != nil {
		return nil, err
	}

	res := make([]os.DirEntry, 0)

	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), LogPrefix) {
			continue
		}

		res = append(res, entry)
	}

	return res, nil
}

func GetCurrentSegment(logDir string, maxSegmentSize int32) (*os.File, error) {

	segments, err := GetSegmentFiles(logDir)

	if err != nil {
		return nil, err
	}

	segment := segments[len(segments)-1]

	return os.Open(segment.Name())
}
