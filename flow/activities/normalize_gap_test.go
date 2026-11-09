package activities

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestNormalizeHeldByStatus(t *testing.T) {
	for status := range protos.FlowStatus_name {
		s := protos.FlowStatus(status)
		switch s {
		case protos.FlowStatus_STATUS_PAUSED, protos.FlowStatus_STATUS_PAUSING, protos.FlowStatus_STATUS_FAILED,
			protos.FlowStatus_STATUS_TERMINATING, protos.FlowStatus_STATUS_TERMINATED, protos.FlowStatus_STATUS_COMPLETED:
			require.True(t, normalizeHeldByStatus(s), "%s doesn't alert", s)
		default:
			require.False(t, normalizeHeldByStatus(s), "%s alerts", s)
		}
	}
}
