package commitlog

import (
	"encoding/binary"
	"errors"
)

// Record contains a mutation which has been persisted to disk.
//
// To ensure that records can be replayed in the correct order, each record will receive an monotonic
// log sequence number (LSN). The LSN is a 64bit unsigned integer which the first 32bits specify in which
// log segment file the record exist and the last 32 bits specify the records index in the file.
//
// When persisting the record the checksum of the mutation is calculated and persisted aswell (CRC), in order
// to ensure that the data is valid when replaying the records. The persisted checksum is compared to the
// checksum of the data field
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
