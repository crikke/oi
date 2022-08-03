package server

import (
	"encoding/gob"
	"io"
)

const metdataPrefix = "db_"

type DbMetadata struct {
	DbName string
}

func loadMetadata(r io.Reader) (DbMetadata, error) {

	m := DbMetadata{}
	dec := gob.NewDecoder(r)

	if err := dec.Decode(&m); err != nil {
		return DbMetadata{}, err
	}

	return m, nil
}
