package lsmtree

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

// TODO: properly handle case when only 1 segment
func getSummaryEntry(rd io.Reader, key []byte) (summaryEntry, error) {

	r := bufio.NewReader(rd)

	var prev *summaryEntry
	for {
		cur := &summaryEntry{}
		if err := cur.readFrom(r); err != nil {
			if errors.Is(err, io.EOF) && prev != nil {
				return *prev, nil
			}

			return summaryEntry{}, err
		}

		if prev != nil {
			if bytes.Compare(prev.key, key) == -1 && bytes.Compare(key, cur.key) == 1 {
				return *prev, nil
			}
		}

		prev = cur

	}

	return summaryEntry{}, nil
}
