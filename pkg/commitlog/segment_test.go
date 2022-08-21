package commitlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_get_last_applied_segment(t *testing.T) {

	segment := uint64(101 << 32)
	// record
	segment += 123

	assert.Equal(t, uint32(101), SegmentNumber(segment))
}

func Test_get_last_applied_record(t *testing.T) {

	segment := uint64(101 << 32)
	// record
	segment += 123

	assert.Equal(t, 123, RecordNumber(segment))
}
