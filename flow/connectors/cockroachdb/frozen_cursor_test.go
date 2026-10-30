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

func TestFrozenCursorTracker(t *testing.T) {
	t0 := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	c1, c2 := crdbHLC{WallNanos: 1}, crdbHLC{WallNanos: 2}
	s1 := frozenCursorSettings{idleTimeout: 60 * time.Second, maxBatchSize: 250000}
	s2 := frozenCursorSettings{idleTimeout: 300 * time.Second, maxBatchSize: 250000}
	frozen := func(f *frozenCursorTracker, cursor crdbHLC, settings frozenCursorSettings, now time.Time) bool {
		ok, _, _ := f.frozen(cursor, settings, now)
		return ok
	}
	var f frozenCursorTracker

	// 20 forced batches on the same cursor, but within 15 minutes: not yet
	for i := range frozenCursorMinBatches {
		f.observe(true, false, c1, c1, s1, t0.Add(time.Duration(i)*10*time.Second))
	}
	require.False(t, frozen(&f, c1, s1, t0.Add(4*time.Minute)))
	// frozen once 15 minutes have passed, and stays so for every retry from that cursor with those settings
	f.observe(true, false, c1, c1, s1, t0.Add(16*time.Minute))
	require.True(t, frozen(&f, c1, s1, t0.Add(16*time.Minute)))
	require.True(t, frozen(&f, c1, s1, t0.Add(2*time.Hour)))
	// but not with other settings (the user resumed with a longer sync interval) or from another cursor (resync)
	require.False(t, frozen(&f, c1, s2, t0.Add(16*time.Minute)))
	require.False(t, frozen(&f, c2, s1, t0.Add(16*time.Minute)))
	// a batch with the new settings that still replays starts counting again
	f.observe(true, false, c1, c1, s2, t0.Add(17*time.Minute))
	require.False(t, frozen(&f, c1, s2, t0.Add(40*time.Minute)))

	// one batch advancing the cursor resets it
	for i := range frozenCursorMinBatches {
		f.observe(true, false, c1, c1, s1, t0.Add(time.Duration(i)*time.Minute))
	}
	f.observe(true, false, c1, c2, s1, t0.Add(21*time.Minute))
	require.False(t, frozen(&f, c1, s1, t0.Add(40*time.Minute)))

	// a batch that ended on a resolved timestamp resets it
	for i := range frozenCursorMinBatches {
		f.observe(true, false, c2, c2, s1, t0.Add(time.Duration(i)*time.Minute))
	}
	f.observe(false, false, c2, c2, s1, t0.Add(30*time.Minute))
	require.False(t, frozen(&f, c2, s1, t0.Add(30*time.Minute)))

	// long but few batches (a slow catch-up with a big sync interval): not frozen
	f.observe(true, false, c2, c2, s1, t0)
	f.observe(true, false, c2, c2, s1, t0.Add(time.Hour))
	require.False(t, frozen(&f, c2, s1, t0.Add(time.Hour)))
}

func TestFrozenCursorForOutlivesTheConnector(t *testing.T) {
	a := frozenCursorFor("TestFrozenCursorForOutlivesTheConnector")
	require.Same(t, a, frozenCursorFor("TestFrozenCursorForOutlivesTheConnector"))
	require.NotSame(t, a, frozenCursorFor("TestFrozenCursorForOutlivesTheConnector_other"))
}

// A batch from a frozen cursor runs strict: it never emits past the checkpoint, and it ends as soon as a resolved
// timestamp advances the cursor, so the mirror recovers by itself once the cause is gone.
func TestStrictBatchWaitsForResolved(t *testing.T) {
	ctx := t.Context()
	om, _ := newMetricsTestOtelManager(t)
	c := &CockroachDBConnector{logger: log.NewStructuredLogger(slog.Default())}
	state := newProcessRowTestState()
	base := time.Now().UnixNano()
	frozenAt := crdbHLC{WallNanos: base}
	state.cursor, state.strictFrom, state.strictUntil = frozenAt, frozenAt, time.Now().Add(time.Hour)
	state.batchDeadline = time.Now().Add(-time.Hour) // long past: a normal batch would force-emit now

	row := &changefeedEnvelope{
		After:   map[string]json.RawMessage{"id": json.RawMessage(`1`)},
		Updated: fmt.Sprintf("%d.0000000000", base+10),
	}
	require.NoError(t, c.processChangefeedRow(ctx, om, state, "defaultdb.public.users", []byte("[1]"), row, 10))
	require.LessOrEqual(t, state.watchdogWindow(), max(30*time.Second, 5*state.resolvedInterval),
		"past deadlines don't shrink a strict batch's watchdog")

	// a resolved timestamp at the frozen cursor neither emits nor ends the batch
	done, err := c.handleResolved(ctx, om, state, frozenAt.String())
	require.NoError(t, err)
	require.False(t, done)
	require.Zero(t, state.recordCount)
	require.False(t, state.forcedEmit)

	// one past the row emits it under the checkpoint and ends the batch
	done, err = c.handleResolved(ctx, om, state, fmt.Sprintf("%d.0000000000", base+20))
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, uint32(1), state.recordCount)
	require.False(t, state.forcedEmit, "nothing was emitted past the checkpoint")

	// filling the buffer can't force-emit either: the batch fails instead, having emitted nothing
	strict := newProcessRowTestState()
	strict.cursor, strict.strictFrom, strict.strictUntil = frozenAt, frozenAt, time.Now().Add(time.Hour)
	strict.bufferedBytes = changefeedBufferSoftLimitBytes
	err = c.processChangefeedRow(ctx, om, strict, "defaultdb.public.users", []byte("[1]"), row, 10)
	require.ErrorIs(t, err, errStrictWaitOver)
	require.Zero(t, strict.recordCount)
}

// A batch that held for a resolved timestamp as long as it may and still force-emitted counts as frozen at once,
// instead of after 20 such batches (20 hours).
func TestFrozenCursorTrackerCappedHold(t *testing.T) {
	t0 := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	c1, c2 := crdbHLC{WallNanos: 1}, crdbHLC{WallNanos: 2}
	s1 := frozenCursorSettings{idleTimeout: 60 * time.Second, maxBatchSize: 250000}
	var f frozenCursorTracker
	f.observe(true, true, c1, c1, s1, t0.Add(time.Hour))
	ok, _, batches := f.frozen(c1, s1, t0.Add(time.Hour))
	require.True(t, ok)
	require.Equal(t, frozenCursorMinBatches, batches)
	// a capped batch that still advanced the cursor resets it as any other
	f.observe(true, true, c1, c2, s1, t0.Add(2*time.Hour))
	ok, _, _ = f.frozen(c1, s1, t0.Add(2*time.Hour))
	require.False(t, ok)
}
