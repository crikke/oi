package commitlog

import (
	"encoding/binary"
	"errors"
)

type Record struct {
	Data       []byte
	DataLength uint32
	Crc        uint32
	// first 32 bits are in which segment the record existing
	// last 32 bits are specify order of the records in the segment
	LSN uint64
}

func (r Record) MarshalBinary() ([]byte, error) {

	data := make([]byte, 12)

	binary.LittleEndian.PutUint64(data[0:4], r.LSN)
	binary.LittleEndian.PutUint32(data[4:8], r.DataLength)

	if r.Crc == uint32(0) {
		return nil, errors.New("record missing checksum")
	}

	binary.LittleEndian.PutUint32(data[8:12], r.Crc)

	data = append(data, r.Data...)
	return data, nil
}
