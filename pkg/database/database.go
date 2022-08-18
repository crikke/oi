package database

import "github.com/crikke/oi/pkg/memtree"

type Database struct {
	Memcache *memtree.Memtree
}
