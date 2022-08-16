package commitlog

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

func ReadLogSegment(f *os.File) []Record {

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

		r.dataLength = binary.LittleEndian.Uint32(dataLen)

		crc := make([]byte, 4)
		if _, err := f.Read(crc); err != nil {
			panic(err)
		}

		r.crc = binary.LittleEndian.Uint32(crc)

		data := make([]byte, r.dataLength)

		if _, err := f.Read(data); err != nil {
			panic(err)
		}

		r.data = data

		records = append(records, r)
	}
	return records
}
