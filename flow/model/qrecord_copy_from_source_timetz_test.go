package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimetzBinary(t *testing.T) {
	// 12:34:56.789 UTC: int64 microseconds, then a zero zone
	require.Equal(t,
		[]byte{0, 0, 0, 0x0a, 0x8b, 0xe6, 0x26, 0x08, 0, 0, 0, 0},
		timetzBinary(12*time.Hour+34*time.Minute+56*time.Second+789*time.Millisecond))
}
