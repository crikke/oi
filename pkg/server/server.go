package server

import (
	"fmt"
	"log"
	"os"
	"strings"

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
	databases     []*db
}

func (s Server) Start() {

	if err := s.loadDatabaseMetadata(); err != nil {
		log.Fatal(err)
	}

	for _, db := range s.databases {
		ensureDirExists(fmt.Sprintf("%s/%s", s.Configuration.Directory.Log, db.metadata.DbName))
		ensureDirExists(fmt.Sprintf("%s/%s", s.Configuration.Directory.Data, db.metadata.DbName))
		// When starting Commitlog manager
		// check db for last applied record
		// handle replaying records not applied yet
		// once done start accepting reads & writes

	}
}

func (s Server) loadDatabaseMetadata() error {
	ensureDirExists(s.Configuration.Directory.Metadata)

	entries, err := os.ReadDir(s.Configuration.Directory.Metadata)

	if err != nil {
		panic(err)
	}

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

			s.databases = append(s.databases, &db{metadata: m})
		}
	}
	return nil
}

func ensureDirExists(dir string) {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0664)
	}
}
