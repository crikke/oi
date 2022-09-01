package database

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/crikke/oi/pkg/data/commitlog"
	"github.com/crikke/oi/pkg/data/lsmtree"
	"github.com/crikke/oi/pkg/data/lsmtree/memtree"
	pb "github.com/crikke/oi/proto-gen/data"
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
	lsmTree        *lsmtree.LSMTree
	configuration  Configuration
	Descriptor     *Descriptor
	writer         *commitlog.Writer
	cancelFunc     func()
	descriptorPath string
}

func CreateDatabase(descriptorDir, name string) (*Database, error) {
	d := Descriptor{
		Name: name,
		UUID: uuid.New(),
	}

	filename := fmt.Sprintf("%s%s", d.UUID.String(), DescriptorPrefix)

	if _, err := os.Stat(filepath.Join(descriptorDir, filename)); !errors.Is(err, os.ErrNotExist) {

		if err != nil {
			return nil, err
		}

		return nil, errors.New("descriptor exists")
	}

	f, err := os.OpenFile(filepath.Join(descriptorDir, filename), os.O_CREATE|os.O_APPEND, 0660)

	if err != nil {
		return nil, err
	}

	if err := encodeDescriptor(f, d); err != nil {
		return nil, err
	}

	if err = f.Close(); err != nil {
		return nil, err
	}
	return nil, nil
}

func OpenDatabase(descriptorPath string, c Configuration) (*Database, error) {

	f, err := os.Open(descriptorPath)

	if err != nil {
		return nil, err
	}

	m, err := decodeDescriptor(f)
	if err != nil {
		return nil, err
	}
	db := &Database{
		Descriptor:     &m,
		configuration:  c,
		descriptorPath: descriptorPath,
	}
	if err = f.Close(); err != nil {
		return nil, err
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

	db.lsmTree = lsmtree.NewLSMTree(&lsmtree.Configuration{
		DataDir:        db.configuration.Directory.Data,
		MemtreeMaxSize: uint32(db.configuration.Memtree.MaxSize),
	})

	db.cancelFunc = cancel
	w, err := commitlog.NewWriter(ctx, db.configuration.Directory.Log, int(db.configuration.Commitlog.SegmentSize), db.lsmTree.Append)

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

func (db *Database) Put(ctx context.Context, key, value []byte) error {

	m := &pb.Mutation{
		Key:       key,
		Value:     value,
		Tombstone: nil,
	}
	return db.writer.Write(m)
}

func (db *Database) Get(ctx context.Context, key []byte) ([]byte, error) {

	value, err := db.lsmTree.Get(key)

	return value, nil
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
			return nil
		default:

			// skip applied records
			if record.LSN <= descriptor.LastAppliedRecord {
				continue
			}
			m := &commitlog.Mutation{}
			m.UnmarshalBinary(record.Data)
			db.memtable.Put(m.Key, m.Value)

		}
	}
	return nil
}

func decodeDescriptor(r io.Reader) (Descriptor, error) {

	m := Descriptor{}
	dec := gob.NewDecoder(r)

	if err := dec.Decode(&m); err != nil {
		return Descriptor{}, err
	}

	return m, nil
}

func encodeDescriptor(w io.Writer, d Descriptor) error {

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
