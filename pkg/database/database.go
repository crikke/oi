package database

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/crikke/oi/pkg/commitlog"
	"github.com/crikke/oi/pkg/memtree"
	"github.com/google/uuid"
)

const DescriptorPrefix = "db_"

type Descriptor struct {
	Name string
	// UUID
	UUID uuid.UUID
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
	cancelFunc    func()
}

// Init Initializes the database from the descriptor.
//
// When starting the database the commitlog writer will start
// When the writer is started, records who havent been applied are replayed and inserted into the Memtable
//
func Init(descriptor Descriptor, c Configuration) (*Database, error) {
	ctx, cancel := context.WithCancel(context.Background())
	ensureDirExists(fmt.Sprintf("%s/%s", c.Directory.Log, descriptor.Name))
	ensureDirExists(fmt.Sprintf("%s/%s", c.Directory.Data, descriptor.Name))
	// When starting Commitlog manager
	db := &Database{cancelFunc: cancel}
	mc, err := memtree.Initalize(c.Memtree)

	if err != nil {
		log.Fatal(err)
	}
	db.Memtable = &mc

	w, err := commitlog.NewWriter(ctx, db.Configuration.Directory.Log, int(db.Configuration.Commitlog.SegmentSize))

	if err != nil {
		return nil, fmt.Errorf("[Init] Fatal: %w", err)
	}
	db.writer = w

	db.ensureRecordsAreApplied(ctx)
	return nil, nil

}

func (d *Database) ensureRecordsAreApplied(ctx context.Context) error {

	if d.descriptor.LastAppliedRecord > 0 {
		segmentFiles, err := commitlog.GetTrailingSegments(d.Configuration.Directory.Log, d.descriptor.LastAppliedRecord)

		if err != nil {
			return fmt.Errorf("[ensureRecordsAreApplied] fatal: %w", err)
		}

		for _, segment := range segmentFiles {

			select {
			case <-ctx.Done():
				log.Println("cancelled applying records")
				return nil
			default:
				if err := replaySegment(ctx, segment, d, d.descriptor); err != nil {
					return fmt.Errorf("[ensureRecordsAreApplied] fatal: %w", err)
				}
			}
		}
	}
	return nil
}

// Close flushes the memtable to disk
func (d *Database) Close() error {
	d.cancelFunc()
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
	return nil
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
