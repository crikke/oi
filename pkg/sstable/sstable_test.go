package sstable

import (
	"bytes"
	"encoding/binary"
	"testing"

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

	encodeEntry(b, e)

	actualLength := binary.LittleEndian.Uint16(b.Next(2))
	assert.Equal(t, e.keyLength, actualLength)

	encodedStr := b.Next(int(e.keyLength))

	assert.Equal(t, str, string(encodedStr))
}
