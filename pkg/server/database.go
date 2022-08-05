package server

import (
	"encoding/gob"
	"io"
	"os"
	"strconv"
	"strings"
)

const (
	metdataPrefix = "db_"
	logPrefix     = "log_"
	logSuffix     = ".log"
)

type DbMetadata struct {
	DbName string
	// The most recent synced (written to SSTable) record.
	LastAppliedRecord uint64
}

func loadMetadata(r io.Reader) (DbMetadata, error) {

	m := DbMetadata{}
	dec := gob.NewDecoder(r)

	if err := dec.Decode(&m); err != nil {
		return DbMetadata{}, err
	}

	return m, nil
}

type db struct {
	metadata DbMetadata
	// TODO: split database specific things from ServerConfiguration
	configuration ServerConfiguration
}

func (d *db) start() error {

	if d.metadata.LastAppliedRecord > 0 {
		lastAppliedSegment := getLastAppliedSegment(d.metadata.LastAppliedRecord)

		segmentFiles, err := getSegmentFiles(d.configuration.Directory.Log)

		if err != nil {
			return err
		}

		segmentsToReplay := make([]os.DirEntry, 0)
		for i, entry := range segmentFiles {
			// remove the actual prefix from segment
			n := entry.Name()[len(logPrefix):]

			if strings.HasPrefix(n, strconv.FormatUint(uint64(lastAppliedSegment), 10)) {
				segmentsToReplay = append(segmentsToReplay, segmentFiles[i:]...)
				break
			}
		}

	}
	return nil
}

func getLastAppliedSegment(lsn uint64) uint32 {

	return uint32(lsn >> 32)
}

func getSegmentFiles(dir string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)

	if err != nil {
		return nil, err
	}

	res := make([]os.DirEntry, 0)

	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), logPrefix) {
			continue
		}

		res = append(res, entry)
	}

	return res, nil
}
