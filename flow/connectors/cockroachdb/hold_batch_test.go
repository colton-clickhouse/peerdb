package conncockroachdb

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
)

// newHoldTestState is a pull that started from cursor at now - elapsed, has records and is past its deadline.
func newHoldTestState(cursor crdbHLC, hold bool, elapsed time.Duration) *changefeedPullState {
	state := newProcessRowTestState()
	now := time.Now()
	state.cursor, state.startCursor = cursor, cursor
	state.pullStart = now.Add(-elapsed)
	state.holdForResolved = hold
	state.resolvedInterval = changefeedResolvedInterval
	state.recordCount = 1
	state.batchDeadline = now.Add(-time.Minute)
	return state
}

func TestHoldBatchUntilCursorAdvances(t *testing.T) {
	cursor := crdbHLC{WallNanos: time.Now().Add(-time.Hour).UnixNano()}
	now := time.Now()

	held := newHoldTestState(cursor, true, 10*time.Minute)
	require.NotEmpty(t, held.batchCutReason(now), "past the deadline")
	require.True(t, held.holdingForResolved(now), "kept open: the cursor hasn't advanced")
	require.Equal(t, changefeedCatchUpStallWindow, held.watchdogWindow())
	held.graceDeadline = now.Add(-time.Second)
	require.True(t, held.holdingForResolved(now), "past a full batch's grace window too")
	require.Equal(t, changefeedCatchUpStallWindow, held.watchdogWindow())

	// a resolved timestamp advanced the cursor: the batch ends as before
	held.cursor = crdbHLC{WallNanos: cursor.WallNanos + 1}
	require.False(t, held.holdingForResolved(now))

	// older mirrors, and a hold that has lasted changefeedCatchUpMaxDuration, end at the deadline
	require.False(t, newHoldTestState(cursor, false, 10*time.Minute).holdingForResolved(now))
	require.False(t, newHoldTestState(cursor, true, changefeedCatchUpMaxDuration+time.Second).holdingForResolved(now))
}

func TestHoldBatchEndsOnAdvancingResolved(t *testing.T) {
	ctx := t.Context()
	om, _ := newMetricsTestOtelManager(t)
	c := &CockroachDBConnector{logger: log.NewStructuredLogger(slog.Default())}
	base := time.Now().Add(-time.Hour).UnixNano()
	state := newHoldTestState(crdbHLC{WallNanos: base}, true, 10*time.Minute)
	state.recordCount = 0
	state.graceDeadline = time.Now().Add(-time.Second)

	row := &changefeedEnvelope{
		After:   map[string]json.RawMessage{"id": json.RawMessage(`1`)},
		Updated: fmt.Sprintf("%d.0000000000", base+2),
	}
	require.NoError(t, c.processChangefeedRow(ctx, om, state, "defaultdb.public.users", []byte("[1]"), row, 10))
	require.NoError(t, c.emitBuffered(ctx, state, nil, "test"))
	require.Equal(t, uint32(1), state.recordCount)

	// a resolved timestamp at the start cursor doesn't end the batch
	done, err := c.handleResolved(ctx, om, state, fmt.Sprintf("%d.0000000000", base))
	require.NoError(t, err)
	require.False(t, done)

	// the first one past it does, and it covers what was emitted
	done, err = c.handleResolved(ctx, om, state, fmt.Sprintf("%d.0000000000", base+3))
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, crdbHLC{WallNanos: base + 3}, state.cursor)
}
