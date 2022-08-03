package database

import (
	"os"
)

const (
	walDirectory = "log"
)

type DatabaseConfiguration struct {
	Name    string
	WALDir  string
	DataDir string
}

type db struct {
	Configuration DatabaseConfiguration
}

func (d *db) NewDatabase() error {

	ensureDirExists(d.Configuration.WALDir)
	ensureDirExists(d.Configuration.DataDir)

	return nil
}

func ensureDirExists(dir string) {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0664)
	}
}
