package database

import (
	"context"
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
	Commitlog struct {
		SegmentSize uint32
	}
	Memtree memtree.Configuration
}

type Database struct {
	Memtable      *memtree.Memtree
	Configuration Configuration
	descriptor    Descriptor
	writer        *commitlog.Writer
}

func Init(ctx context.Context, descriptor Descriptor, c Configuration) (*Database, error) {

	ensureDirExists(fmt.Sprintf("%s/%s", c.Directory.Log, descriptor.Name))
	ensureDirExists(fmt.Sprintf("%s/%s", c.Directory.Data, descriptor.Name))
	// When starting Commitlog manager
	db := &Database{
		closeChannel: make(chan struct{}),
	}
	mc, err := memtree.Initalize(c.Memtree)

	if err != nil {
		log.Fatal(err)
	}
	db.Memtable = &mc

	w := commitlog.NewWriter(ctx, db.Configuration.Directory.Log, int(db.Configuration.Commitlog.SegmentSize))
	db.writer = w

	db.ensureRecordsAreApplied(ctx)
	return nil, nil

}

func (d *Database) ensureRecordsAreApplied(ctx context.Context) error {

	if d.descriptor.LastAppliedRecord > 0 {
		segmentFiles, err := commitlog.GetTrailingSegments(d.Configuration.Directory.Log, d.descriptor.LastAppliedRecord)

		if err != nil {
			fmt.Errorf("[ensureRecordsAreApplied] fatal: %w", err)
		}

		for _, segment := range segmentFiles {

			select {
			case <-ctx.Done():
				log.Println("cancelled applying records")
				return
			default:
					if err := replaySegment(segment, d, d.descriptor); err != nil {
						fmt.Errorf("[ensureRecordsAreApplied] fatal: %w", err)
					}
		}
	}
	return nil
}

func replaySegment(ctx context.Context, s os.DirEntry, db *Database, descriptor Descriptor) error {

	f, err := os.Open(s.Name())
	if err != nil {
		panic(err)
	}
	defer f.Close()

	records, err := commitlog.ReadLogSegment(ctx, f)
	if err != nil {
		return fmt.Errorf("[replaySegment] fatal: %w", err)
	}

	for _, record := range records {
	
		select {
		case <-ctx.Done():
			break
		default:


			// skip applied records 
			if record.LSN <= descriptor.LastAppliedRecord {
				continue
			}
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
