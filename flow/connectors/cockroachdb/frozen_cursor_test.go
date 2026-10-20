package conncockroachdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		f.observe(true, c1, c1, s1, t0.Add(time.Duration(i)*10*time.Second))
	}
	require.False(t, frozen(&f, c1, s1, t0.Add(4*time.Minute)))
	// frozen once 15 minutes have passed, and stays so for every retry from that cursor with those settings
	f.observe(true, c1, c1, s1, t0.Add(16*time.Minute))
	require.True(t, frozen(&f, c1, s1, t0.Add(16*time.Minute)))
	require.True(t, frozen(&f, c1, s1, t0.Add(2*time.Hour)))
	// but not with other settings (the user resumed with a longer sync interval) or from another cursor (resync)
	require.False(t, frozen(&f, c1, s2, t0.Add(16*time.Minute)))
	require.False(t, frozen(&f, c2, s1, t0.Add(16*time.Minute)))
	// a batch with the new settings that still replays starts counting again
	f.observe(true, c1, c1, s2, t0.Add(17*time.Minute))
	require.False(t, frozen(&f, c1, s2, t0.Add(40*time.Minute)))

	// one batch advancing the cursor resets it
	for i := range frozenCursorMinBatches {
		f.observe(true, c1, c1, s1, t0.Add(time.Duration(i)*time.Minute))
	}
	f.observe(true, c1, c2, s1, t0.Add(21*time.Minute))
	require.False(t, frozen(&f, c1, s1, t0.Add(40*time.Minute)))

	// a batch that ended on a resolved timestamp resets it
	for i := range frozenCursorMinBatches {
		f.observe(true, c2, c2, s1, t0.Add(time.Duration(i)*time.Minute))
	}
	f.observe(false, c2, c2, s1, t0.Add(30*time.Minute))
	require.False(t, frozen(&f, c2, s1, t0.Add(30*time.Minute)))

	// long but few batches (a slow catch-up with a big sync interval): not frozen
	f.observe(true, c2, c2, s1, t0)
	f.observe(true, c2, c2, s1, t0.Add(time.Hour))
	require.False(t, frozen(&f, c2, s1, t0.Add(time.Hour)))
}

func TestFrozenCursorForOutlivesTheConnector(t *testing.T) {
	a := frozenCursorFor("TestFrozenCursorForOutlivesTheConnector")
	require.Same(t, a, frozenCursorFor("TestFrozenCursorForOutlivesTheConnector"))
	require.NotSame(t, a, frozenCursorFor("TestFrozenCursorForOutlivesTheConnector_other"))
}
