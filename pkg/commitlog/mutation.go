package commitlog

import "encoding/binary"

type Mutation struct {
	keyLength   uint16
	key         []byte
	valueLength int32
	value       []byte
	tombstone   bool
}

func (m Mutation) MarshalBinary() ([]byte, error) {

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

func makeMutation(key, value []byte, tombstone bool) Mutation {

	data := Mutation{
		value:       value,
		key:         key,
		keyLength:   uint16(len(key)),
		valueLength: int32(len(value)),
		tombstone:   tombstone,
	}

	return data
}
