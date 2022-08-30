package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/crikke/oi/pkg/memtree"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestEncodeEntry(t *testing.T) {

	str := "aaaa"
	key := []byte(str)
	length := uint16(len(key))

	e := indexEntry{
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
	rbt.Insert([]byte("bbb"), []byte("222"))
	rbt.Insert([]byte("aaa"), []byte("111"))
	rbt.Insert([]byte("ddd"), []byte("444"))
	rbt.Insert([]byte("ccc"), []byte("333"))

	iw := &bytes.Buffer{}
	data := &bytes.Buffer{}

	err := traverseRBTree(iw, data, rbt)
	assert.NoError(t, err)

	// bytes are in little endian order
	assert.Equal(t, []byte{3, 0}, iw.Next(2))
	assert.Equal(t, []byte("aaa"), iw.Next(3))
	assert.Equal(t, []byte{0, 0, 0, 0}, iw.Next(4))
	assert.Equal(t, []byte{3, 0}, iw.Next(2))

	assert.Equal(t, []byte{3, 0}, iw.Next(2))
	assert.Equal(t, []byte("bbb"), iw.Next(3))
	assert.Equal(t, []byte{3, 0, 0, 0}, iw.Next(4))
	assert.Equal(t, []byte{3, 0}, iw.Next(2))

	assert.Equal(t, []byte{3, 0}, iw.Next(2))
	assert.Equal(t, []byte("ccc"), iw.Next(3))
	assert.Equal(t, []byte{6, 0, 0, 0}, iw.Next(4))
	assert.Equal(t, []byte{3, 0}, iw.Next(2))

	assert.Equal(t, []byte("111"), data.Next(3))
	assert.Equal(t, []byte("222"), data.Next(3))
	assert.Equal(t, []byte("333"), data.Next(3))

}

func TestDecodeSSTable(t *testing.T) {

	rbt := memtree.RBTree{}

	// assert that entries are stored in order
	rbt.Insert([]byte("bbb"), []byte("222"))
	rbt.Insert([]byte("aaa"), []byte("111"))
	rbt.Insert([]byte("ddd"), []byte("444"))
	rbt.Insert([]byte("ccc"), []byte("333"))

	n := fmt.Sprintf("/tmp/%s", uuid.NewString())

	data, err := os.Create(fmt.Sprintf("%s.data", n))
	if err != nil {
		assert.NoError(t, err)
	}
	defer os.Remove(data.Name())

	idx, err := os.Create(fmt.Sprintf("%s.idx", n))
	if err != nil {
		assert.NoError(t, err)
	}
	defer os.Remove(idx.Name())

	err = traverseRBTree(idx, data, rbt)
	assert.NoError(t, err)

	sst, err := Open(fmt.Sprintf("%s", n))
	defer sst.Close()
	assert.NoError(t, err)

	assert.NoError(t, err)

	val, err := sst.Get([]byte("aaa"))
	assert.NoError(t, err)

	assert.Equal(t, []byte("111"), val)

}
