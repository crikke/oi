package commitlog

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshalBinary(t *testing.T) {

	data := make([]byte, 7)

	binary.LittleEndian.PutUint16(data[0:2], uint16(3))
	binary.LittleEndian.PutUint32(data[2:6], uint32(6))

	data = append(data, []byte("foo")...)
	data = append(data, []byte("barbaz")...)

	m := &Mutation{}

	err := m.UnmarshalBinary(data)

	assert.Len(t, data, 16)
	assert.NoError(t, err)

	assert.Equal(t, []byte("foo"), m.Key)
	assert.Equal(t, []byte("barbaz"), m.Value)
	assert.Equal(t, false, m.tombstone)

	assert.Equal(t, uint16(3), m.keyLength)
	assert.Equal(t, uint32(6), m.valueLength)

}

func TestMarshalBinary(t *testing.T) {

	m := Mutation{
		keyLength:   3,
		Key:         []byte("foo"),
		valueLength: 6,
		Value:       []byte("barbaz"),
		tombstone:   true,
	}

	data, err := m.MarshalBinary()

	assert.NoError(t, err)
	assert.Equal(t, m.keyLength, binary.LittleEndian.Uint16(data[0:2]))
	assert.Equal(t, m.valueLength, binary.LittleEndian.Uint32(data[2:6]))
	assert.Equal(t, uint8(1), data[6])
	assert.Equal(t, []byte("foo"), data[7:10])
	assert.Equal(t, []byte("barbaz"), data[10:16])
}
