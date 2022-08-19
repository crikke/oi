package server

import (
	"log"
	"os"
	"strings"
	"sync"

	"github.com/crikke/oi/pkg/database"
)

// The engine is the main component which orchestrates all other components
// It handles scheduling of SSTable merges & writes, Memtree flushes.
// It also exposes operations for reading & writing data
type ServerConfiguration struct {
	Port int

	Directory struct {
		Metadata string
	}

	Database database.Configuration
}

type Server struct {
	Configuration ServerConfiguration
	databases     []*database.Database
}

func (s Server) Start() {

	descriptors, err := s.loadDatabaseDescriptors()
	if err != nil {
		panic(err)
	}
	s.databases = make([]*database.Database, 0)

	var wg sync.WaitGroup

	wg.Add(len(descriptors))
	for _, descriptor := range descriptors {
		defer wg.Done()

		db, err := database.Init(descriptor, s.Configuration.Database)

		if err != nil {
			panic(err)
		}

		s.databases = append(s.databases, db)
	}

	wg.Wait()
}

func (s Server) loadDatabaseDescriptors() ([]database.Descriptor, error) {
	ensureDirExists(s.Configuration.Directory.Metadata)

	entries, err := os.ReadDir(s.Configuration.Directory.Metadata)

	if err != nil {
		panic(err)
	}

	md := make([]database.Descriptor, 0)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), database.DescriptorPrefix) {

			f, err := os.Open(entry.Name())
			if err != nil {
				log.Fatal(err)
			}
			m, err := database.DecodeDescriptor(f)

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
