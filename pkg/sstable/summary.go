package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// summary is used to determine where in the index a key should exist
// The summary consists of pages which contains a start & end key.
type summary struct {
	samplingSize int
	entries      []summaryEntry
	entriesCount uint32
}

type summaryEntry struct {
	key    []byte
	keyLen uint16
	// position of the key in the index file
	position uint64
}

func (se summaryEntry) MarshalBinary() ([]byte, error) {

	buf := make([]byte, se.keyLen+10)

	binary.LittleEndian.PutUint16(buf[0:2], se.keyLen)
	binary.LittleEndian.PutUint64(buf[2:10], se.position)
	buf = append(buf[10:], se.key...)

	return buf, nil
}

func newSummary(index io.Reader, samplingSize int) (*summary, error) {
	s := &summary{
		samplingSize: samplingSize,
		entries:      make([]summaryEntry, 0),
	}

	r := bufio.NewReader(index)

	i := 0
	pos := 0
	for {
		entry := &indexEntry{}
		n, err := decodeIndexEntry(r, entry)
		pos += n

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		if i%s.samplingSize == 0 {
			se := summaryEntry{
				key:      entry.key,
				keyLen:   entry.keyLength,
				position: uint64(pos),
			}

			s.entries = append(s.entries, se)
		}
	}

	return s, nil
}

func (s *summary) Save(dir string) error {

	f, err := os.OpenFile(dir, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0660)

	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)

	for _, e := range s.entries {
		data, err := e.MarshalBinary()
		if err != nil {
			if ferr := f.Close(); ferr != nil {
				return fmt.Errorf("ferr: %w err:%w", ferr, err)
			}
			return err
		}

		w.Write(data)

	}

	return f.Close()
}

func getSummaryEntry(dir string, key []byte) (summaryEntry, error) {

	f, err := os.Open(dir)
	defer f.Close()

	if err != nil {
		return summaryEntry{}, err
	}

	return summaryEntry{}, nil
}
