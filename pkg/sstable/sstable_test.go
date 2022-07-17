package sstable

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/crikke/oi/pkg/memtree"
	"github.com/stretchr/testify/assert"
)

func TestEncodeEntry(t *testing.T) {

	str := "aaaa"
	key := []byte(str)
	length := uint16(len(key))

	e := entry{
		key:       key,
		keyLength: length,
		position:  0,
	}

	b := &bytes.Buffer{}

	encodeIndexEntry(b, e)

	actualLength := binary.LittleEndian.Uint16(b.Next(2))
	assert.Equal(t, e.keyLength, actualLength)

	encodedStr := b.Next(int(e.keyLength))

	assert.Equal(t, str, string(encodedStr))
}

func TestCreateSSTableIndex(t *testing.T) {

	rbt := memtree.RBTree{}

	// assert that entries are stored in order
	rbt.Insert("bbb", []byte("222"))
	rbt.Insert("aaa", []byte("111"))
	rbt.Insert("ddd", []byte("444"))
	rbt.Insert("ccc", []byte("333"))

	iw := &bytes.Buffer{}
	data := &bytes.Buffer{}

	err := createSSTable(iw, data, rbt)
	assert.NoError(t, err)

	// bytes are in little endian order
	assert.Equal(t, []byte{3, 0}, iw.Next(2))
	assert.Equal(t, []byte("aaa"), iw.Next(3))
	assert.Equal(t, []byte{0, 0, 0, 0}, iw.Next(4))

	assert.Equal(t, []byte{3, 0}, iw.Next(2))
	assert.Equal(t, []byte("bbb"), iw.Next(3))
	assert.Equal(t, []byte{4, 0, 0, 0}, iw.Next(4))

	assert.Equal(t, []byte{3, 0}, iw.Next(2))
	assert.Equal(t, []byte("ccc"), iw.Next(3))
	assert.Equal(t, []byte{8, 0, 0, 0}, iw.Next(4))

	assert.Equal(t, []byte("111"), data.Next(3))
	assert.Equal(t, []byte("\x00"), data.Next(1))

	assert.Equal(t, []byte("222"), data.Next(3))
	assert.Equal(t, []byte("\x00"), data.Next(1))

	assert.Equal(t, []byte("333"), data.Next(3))
	assert.Equal(t, []byte("\x00"), data.Next(1))

}
