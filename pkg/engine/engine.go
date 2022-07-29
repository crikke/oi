package engine

import (
	"errors"
	"fmt"
	"os"

	"github.com/crikke/oi/pkg/memtree"
)

// The engine is the main component which orchestrates all other components
// It handles scheduling of SSTable merges & writes, Memtree flushes.
// It also exposes operations for reading & writing data
type DbConfiguration struct {
	Path    string
	Port    int
	Name    string
	Memtree memtree.Configuration
}

type Db struct {
	Configuration DbConfiguration
}

func (d Db) Start() {

}

func (d Db) ensureDataIntegrity() error {
	path := fmt.Sprintf("%s/%s.log", d.Configuration.Path, d.Configuration.Name)

	_, err := os.Open(path)

	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			panic(err)
		}
	}

	return nil
}
