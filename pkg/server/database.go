package server

import (
	"encoding/gob"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/crikke/oi/pkg/commitlog"
	"github.com/crikke/oi/pkg/memtree"
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
	memcache      *memtree.Memtree
}

func (d *db) start() error {

	mc, err := memtree.Initalize(d.configuration.Memtree)

	if err != nil {
		log.Fatal(err)
	}

	d.memcache = &mc

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

		for _, segment := range segmentsToReplay {
			d.replaySegment(segment)
		}
	}
	return nil
}

func getLastAppliedSegment(lsn uint64) uint32 {

	return uint32(lsn >> 32)
}

func getLastAppliedRecord(lsn uint64) int {
	return int(lsn & 0xffffffff)
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

func (d *db) replaySegment(s os.DirEntry) {

	f, err := os.Open(s.Name())
	if err != nil {
		panic(err)
	}
	defer f.Close()

	records := commitlog.ReadLogSegment(f)

	lastAppliedRecord := getLastAppliedRecord(d.metadata.LastAppliedRecord)

	// skip already applied records

	if records[len(records)-1].LSN == d.metadata.LastAppliedRecord {
		return
	}

	records = records[lastAppliedRecord+1:]

	for _, record := range records {

		m := &commitlog.Mutation{}

		m.UnmarshalBinary(record.Data)
		d.memcache.Put(string(m.Key), m.Value)
	}
}
