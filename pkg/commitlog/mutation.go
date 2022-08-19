package commitlog

import "encoding/binary"

// A Mutation is a descripton of state change
type Mutation struct {
	keyLength   uint16
	Key         []byte
	valueLength uint32
	Value       []byte
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

	data = append(data, m.Key...)
	data = append(data, m.Value...)

	return data, nil
}

func (m *Mutation) UnmarshalBinary(data []byte) error {

	m.keyLength = binary.LittleEndian.Uint16(data[0:2])
	m.valueLength = binary.LittleEndian.Uint32(data[2:6])

	if uint8(data[6]) == 1 {
		m.tombstone = true
	}

	valueStart := 7 + m.keyLength
	m.Key = data[7:valueStart]
	m.Value = data[valueStart:]

	return nil
}

func makeMutation(key, value []byte, tombstone bool) Mutation {

	data := Mutation{
		Value:       value,
		Key:         key,
		keyLength:   uint16(len(key)),
		valueLength: uint32(len(value)),
		tombstone:   tombstone,
	}

	return data
}
