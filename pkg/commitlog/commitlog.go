package commitlog

import (
	"time"
)

type Commitlog struct {
}

type Entry struct {
	Timestamp time.Time
	Key       []byte
	Value     []byte
}
