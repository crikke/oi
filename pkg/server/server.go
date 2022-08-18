package server

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/crikke/oi/pkg/database"
	"github.com/crikke/oi/pkg/memtree"
)

// The engine is the main component which orchestrates all other components
// It handles scheduling of SSTable merges & writes, Memtree flushes.
// It also exposes operations for reading & writing data
type ServerConfiguration struct {
	Port    int
	Memtree memtree.Configuration

	Directory struct {
		Data     string
		Log      string
		Metadata string
	}
}

type Server struct {
	Configuration ServerConfiguration
	databases     []*database.Database
}

func (s Server) Start() {

	descriptors, err := s.loadDatabaseMetadata()
	if err != nil {
		panic(err)
	}
	s.databases = make([]*database.Database, 0)

	var wg sync.WaitGroup

	wg.Add(len(descriptors))
	for _, descriptor := range descriptors {
		defer wg.Done()
		ensureDirExists(fmt.Sprintf("%s/%s", s.Configuration.Directory.Log, descriptor.DbName))
		ensureDirExists(fmt.Sprintf("%s/%s", s.Configuration.Directory.Data, descriptor.DbName))
		// When starting Commitlog manager
		// check db for last applied record
		// handle replaying records not applied yet
		// once done start accepting reads & writes

		db, err := s.initDatabase(descriptor)

		if err != nil {
			panic(err)
		}

		s.databases = append(s.databases, db)
	}

	wg.Wait()
}

func (s Server) loadDatabaseMetadata() ([]DbMetadata, error) {
	ensureDirExists(s.Configuration.Directory.Metadata)

	entries, err := os.ReadDir(s.Configuration.Directory.Metadata)

	if err != nil {
		panic(err)
	}

	md := make([]DbMetadata, 0)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), metdataPrefix) {

			f, err := os.Open(entry.Name())
			if err != nil {
				log.Fatal(err)
			}
			m, err := loadMetadata(f)

			if err != nil {
				log.Fatal(err)
			}
			md = append(md, m)
		}
	}
	return md, nil
}

func ensureDirExists(dir string) {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0664)
	}
}
