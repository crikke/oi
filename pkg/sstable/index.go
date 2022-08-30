package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

// the entries are variable length due to key
// should the entry length be stored in a array in the beginning to ease creating summary files?
//
// this would make it faster since every entry offset is known and the summary can just take the entries needed
// instead of reading the whole index
// maybe should look into this in the future
type index struct {
	entries []indexEntry
}

type indexEntry struct {
	key       []byte
	keyLength uint16
	position  int64
}

// TODO: this should be  MarshalBinary
func encodeIndexEntry(w io.Writer, e indexEntry) error {

	buf := make([]byte, 8)

	binary.LittleEndian.PutUint16(buf[0:2], e.keyLength)
	binary.LittleEndian.PutUint64(buf[2:8], uint64(e.position))

	buf = append(buf, e.key...)

	if _, err := w.Write(buf); err != nil {
		return err
	}

	return nil
}

func getIndexEntry(dir string, key []byte, offset int64) (indexEntry, error) {

	f, err := os.Open(dir)
	defer f.Close()

	if err != nil {
		return indexEntry{}, err
	}

	_, err = f.Seek(offset, 0)
	if err != nil {
		return indexEntry{}, err
	}

	r := bufio.NewReader(f)
	for {
		e := &indexEntry{}
		_, err := decodeIndexEntry(r, e)

		if err != nil {
			return indexEntry{}, err
		}

		if bytes.Equal(e.key, key) {
			return *e, nil
		}

		// since the index is sorted, just decode entries until entry key is larger than the key to look for.
		// if the entry key is larger then the key cannot exist in the index
		if bytes.Compare(e.key, key) == 1 {
			return indexEntry{}, ErrKeyNotFound
		}
	}
}

func decodeIndexEntry(r io.Reader, e *indexEntry) (int, error) {

	buf := make([]byte, 8)
	n := 0
	ln, err := r.Read(buf)
	n += ln
	if err != nil {
		return n, err
	}
	e.keyLength = binary.LittleEndian.Uint16(buf[0:2])
	e.position = int64(binary.LittleEndian.Uint64(buf[2:8]))

	key := make([]byte, e.keyLength)

	ln, err = r.Read(key)

	n += ln

	if err != nil {
		return n, err
	}

	return n, nil
}
