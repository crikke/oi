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

func TestCreateSSTable(t *testing.T) {

	rbt := memtree.RBTree{}

	rbt.Insert("aaa", []byte("111"))
	rbt.Insert("bbb", []byte("222"))
	rbt.Insert("ccc", []byte("333"))
	rbt.Insert("ddd", []byte("444"))

	iw := &bytes.Buffer{}
	data := &bytes.Buffer{}

	err := createSSTable(iw, data, rbt)
	assert.NoError(t, err)

}
