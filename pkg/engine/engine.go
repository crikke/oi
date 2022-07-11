package engine

import "github.com/crikke/oi/pkg/memtree"

// The engine is the main component which orchestrates all other components
// It handles scheduling of SSTable merges & writes, Memtree flushes.
// It also exposes operations for reading & writing data
type EngineConfiguration struct {
	Path struct {
		// parent directory where the db files are located
		Data string
	}
	Port    int
	Memtree memtree.Configuration
}

type Engine struct {
}

func (e Engine) Start() {

}
