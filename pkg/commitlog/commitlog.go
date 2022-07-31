package commitlog

import (
	"encoding/binary"
	"hash/crc32"
	"sync"
)

// In the event restarting the database because of failure, get internal.record which is the last applied mutation
// When persisting the Memtree to an SSTable on disk, update internal.record

// A record holds an mutation which is when state changes (insert, update, delete)
//
// When writing a record to disk, it will calculate the checksum for the mutation
// and get a lsn which is a monotonic number that is used to replay records in the event of failure
type record struct {
	data       []byte
	dataLength uint32
	crc        uint32
	lsn        uint32
}

type mutation struct {
	keyLength   uint16
	key         []byte
	valueLength int32
	value       []byte
	tombstone   bool
}

type commitlogWriter struct {
	mu      sync.Mutex
	counter int32

	writerChannel chan record
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

func makeRecord(m mutation) (record, error) {

	data, err := m.MarshalBinary()
	if err != nil {
		return record{}, err
	}

	r := record{
		data:       data,
		crc:        crc32.ChecksumIEEE(data),
		dataLength: uint32(len(data)),
	}

	return r, nil
}

func (c *commitlogWriter) startCommitLogLoop() error {

	for {

		r, ok := <-c.writerChannel
		if !ok {
			break
		}

		c.mu.Lock()
		r.lsn = uint32(c.counter)
		c.counter++
		c.mu.Unlock()

	}

	return nil
}

func append(r record) {

}
