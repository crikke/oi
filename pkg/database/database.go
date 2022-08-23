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

// Descriptor holds metadata about the database
type Descriptor struct {
	Name string
	// UUID
	UUID uuid.UUID
	// The most recent synced (written to SSTable) record.
	LastAppliedRecord uint64
	Stopped           bool
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
	memtable       *memtree.Memtree
	configuration  Configuration
	Descriptor     *Descriptor
	writer         *commitlog.Writer
	cancelFunc     func()
	descriptorPath string
}

func NewDatabase(descriptorPath string, c Configuration) (*Database, error) {

	f, err := os.Open(descriptorPath)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	m, err := DecodeDescriptor(f)
	if err != nil {
		return nil, err
	}
	db := &Database{
		Descriptor:     &m,
		configuration:  c,
		descriptorPath: descriptorPath,
	}
	return db, nil
}

// Start the database from the descriptor.
//
// When starting the database the commitlog writer will start
// When the writer is started, records who havent been applied are replayed and inserted into the Memtable
func (db *Database) Start() error {

	ctx, cancel := context.WithCancel(context.Background())
	ensureDirExists(fmt.Sprintf("%s/%s", db.configuration.Directory.Log, db.Descriptor.Name))
	ensureDirExists(fmt.Sprintf("%s/%s", db.configuration.Directory.Data, db.Descriptor.Name))
	db.memtable = memtree.NewMemtree(db.configuration.Memtree.MaxSize)
	db.cancelFunc = cancel
	w, err := commitlog.NewWriter(ctx, db.configuration.Directory.Log, int(db.configuration.Commitlog.SegmentSize))

	if err != nil {
		return fmt.Errorf("[Init] Fatal: %w", err)
	}
	db.writer = w

	db.ensureRecordsAreApplied(ctx)
	return nil

}

func (d *Database) ensureRecordsAreApplied(ctx context.Context) error {

	if d.Descriptor.LastAppliedRecord > 0 {
		segmentFiles, err := commitlog.GetTrailingSegments(d.configuration.Directory.Log, d.Descriptor.LastAppliedRecord)

		if err != nil {
			return fmt.Errorf("[ensureRecordsAreApplied] fatal: %w", err)
		}

		for _, segment := range segmentFiles {

			select {
			case <-ctx.Done():
				log.Println("cancelled applying records")
				return nil
			default:
				if err := replaySegment(ctx, segment, d, *d.Descriptor); err != nil {
					return fmt.Errorf("[ensureRecordsAreApplied] fatal: %w", err)
				}
			}
		}
	}
	return nil
}

// Close flushes the memtable to disk and is called when the server is shutting down.
func (d *Database) Close() error {
	d.cancelFunc()
	return nil
}

// Stop the database manually. When server restarts, the database wont be started automatically.
func (d *Database) Stop() error {
	// even if database fails to close, set stopped to true.
	// this is done so next time the server is starting the database will be stopped.
	d.Descriptor.Stopped = true

	if err := d.Close(); err != nil {
		// TODO: implement logger
		panic(err)
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
			db.memtable.Put(string(m.Key), m.Value)

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

func EncodeDescriptor(w io.Writer, d Descriptor) error {

	enc := gob.NewEncoder(w)

	if err := enc.Encode(d); err != nil {
		return err
	}
	return nil
}

func ensureDirExists(dir string) {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0664)
	}
}
