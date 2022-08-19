package database

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/crikke/oi/pkg/commitlog"
	"github.com/crikke/oi/pkg/memtree"
)

const DescriptorPrefix = "db_"

type Descriptor struct {
	Name string
	// The most recent synced (written to SSTable) record.
	LastAppliedRecord uint64
}

type Configuration struct {
	Directory struct {
		Data string
		Log  string
	}

	Memtree memtree.Configuration
}

type Database struct {
	Memtable      *memtree.Memtree
	Configuration Configuration
	descriptor    Descriptor
}

func Init(descriptor Descriptor, c Configuration) (*Database, error) {

	ensureDirExists(fmt.Sprintf("%s/%s", c.Directory.Log, descriptor.Name))
	ensureDirExists(fmt.Sprintf("%s/%s", c.Directory.Data, descriptor.Name))
	// When starting Commitlog manager
	db := &Database{}

	mc, err := memtree.Initalize(c.Memtree)

	if err != nil {
		log.Fatal(err)
	}
	db.Memtable = &mc

	return nil, nil
}

func (d *Database) ensureRecordsAreApplied() error {

	if d.descriptor.LastAppliedRecord > 0 {
		lastAppliedSegment := commitlog.GetLastAppliedSegment(d.descriptor.LastAppliedRecord)

		segmentFiles, err := commitlog.GetSegmentFiles(d.Configuration.Directory.Log)

		if err != nil {
			return err
		}

		segmentsToReplay := make([]os.DirEntry, 0)
		for i, entry := range segmentFiles {
			// remove the actual prefix from segment
			n := entry.Name()[len(commitlog.LogPrefix):]

			if strings.HasPrefix(n, strconv.FormatUint(uint64(lastAppliedSegment), 10)) {
				segmentsToReplay = append(segmentsToReplay, segmentFiles[i:]...)
				break
			}
		}

		for _, segment := range segmentsToReplay {
			replaySegment(segment, d, d.descriptor)
		}
	}
	return nil
}

func replaySegment(s os.DirEntry, db *Database, descriptor Descriptor) {

	f, err := os.Open(s.Name())
	if err != nil {
		panic(err)
	}
	defer f.Close()

	records := commitlog.ReadLogSegment(f)

	lastAppliedRecord := commitlog.GetLastAppliedRecord(descriptor.LastAppliedRecord)

	// skip already applied records

	if records[len(records)-1].LSN == descriptor.LastAppliedRecord {
		return
	}

	records = records[lastAppliedRecord+1:]

	for _, record := range records {

		m := &commitlog.Mutation{}
		m.UnmarshalBinary(record.Data)
		db.Memtable.Put(string(m.Key), m.Value)
	}
}

func DecodeDescriptor(r io.Reader) (Descriptor, error) {

	m := Descriptor{}
	dec := gob.NewDecoder(r)

	if err := dec.Decode(&m); err != nil {
		return Descriptor{}, err
	}

	return m, nil
}

func ensureDirExists(dir string) {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0664)
	}
}
