package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_get_last_applied_segment(t *testing.T) {

	segment := uint64(101 << 32)
	// record
	segment += 123

	assert.Equal(t, uint32(101), getLastAppliedSegment(segment))
}
