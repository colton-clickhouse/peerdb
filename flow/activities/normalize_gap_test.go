package activities

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestNormalizeGapExceeded(t *testing.T) {
	const threshold = 240
	over := 241 * time.Minute
	for _, tc := range []struct {
		name      string
		status    protos.FlowStatus
		lag       time.Duration
		threshold uint32
		alert     bool
	}{
		{"running, over the threshold", protos.FlowStatus_STATUS_RUNNING, over, threshold, true},
		{"running, at the threshold", protos.FlowStatus_STATUS_RUNNING, threshold * time.Minute, threshold, false},
		{"running, no batches waiting", protos.FlowStatus_STATUS_RUNNING, 0, threshold, false},
		{"threshold 0 disables", protos.FlowStatus_STATUS_RUNNING, over, 0, false},
		{"snapshot (an added table's)", protos.FlowStatus_STATUS_SNAPSHOT, over, threshold, true},
		{"paused", protos.FlowStatus_STATUS_PAUSED, over, threshold, false},
		{"pausing", protos.FlowStatus_STATUS_PAUSING, over, threshold, false},
		{"failed", protos.FlowStatus_STATUS_FAILED, over, threshold, false},
		{"terminating", protos.FlowStatus_STATUS_TERMINATING, over, threshold, false},
		{"completed", protos.FlowStatus_STATUS_COMPLETED, over, threshold, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.alert, normalizeGapExceeded(tc.status, tc.lag, tc.threshold))
		})
	}
}
