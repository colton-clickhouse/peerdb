package conncockroachdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFrozenCursorTracker(t *testing.T) {
	t0 := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	c1, c2 := crdbHLC{WallNanos: 1}, crdbHLC{WallNanos: 2}
	var f frozenCursorTracker

	// 20 forced batches on the same cursor, but within 15 minutes: not yet
	for i := range frozenCursorMinBatches {
		f.observe(true, c1, c1, t0.Add(time.Duration(i)*10*time.Second))
	}
	require.False(t, f.frozen(t0.Add(4*time.Minute)))
	// still frozen once 15 minutes have passed
	f.observe(true, c1, c1, t0.Add(16*time.Minute))
	require.True(t, f.frozen(t0.Add(16*time.Minute)))

	// one batch advancing the cursor resets it
	f.observe(true, c1, c2, t0.Add(17*time.Minute))
	require.False(t, f.frozen(t0.Add(40*time.Minute)))

	// a batch that ended on a resolved timestamp resets it
	for i := range frozenCursorMinBatches {
		f.observe(true, c2, c2, t0.Add(time.Duration(i)*time.Minute))
	}
	f.observe(false, c2, c2, t0.Add(30*time.Minute))
	require.False(t, f.frozen(t0.Add(30*time.Minute)))

	// long but few batches (a slow catch-up with a big sync interval): not frozen
	f.observe(true, c2, c2, t0)
	f.observe(true, c2, c2, t0.Add(time.Hour))
	require.False(t, f.frozen(t0.Add(time.Hour)))
}
