package conncockroachdb

import (
	"errors"
	"sync"
	"time"
)

// A batch that ends without a covering resolved timestamp force-emits its buffer and leaves the stored cursor where
// it was, so the next batch restarts the changefeed from the same cursor and replays. When the replay itself
// outlasts the batch window that repeats forever: rows keep landing, the mirror looks healthy, and nothing after
// the cursor ever arrives (when a replay outlasts the batch window). frozenCursorTracker spots that pattern so
// PullRecords stops replaying and, if no resolved timestamp comes, raises a user-actionable error instead.
const (
	frozenCursorMinBatches  = 20
	frozenCursorMinDuration = 15 * time.Minute
	// frozenCursorStrictWait is how long a batch from a frozen cursor waits for a resolved timestamp before failing
	frozenCursorStrictWait = 15 * time.Minute
)

// errStrictWaitOver ends a strict batch (changefeedPullState.strictUntil) that got no covering resolved timestamp in
// time, or would have had to emit past the checkpoint to bound its buffer
var errStrictWaitOver = errors.New("no covering resolved timestamp within the strict wait")

// frozenCursorSettings are the sync settings the replay happened under: changing them (a longer sync interval or a
// larger batch, the fix the error suggests) gives the mirror a fresh chance.
type frozenCursorSettings struct {
	idleTimeout  time.Duration
	maxBatchSize uint32
}

type frozenCursorTracker struct {
	mu       sync.Mutex
	cursor   crdbHLC
	settings frozenCursorSettings
	since    time.Time
	batches  int
}

// frozenCursors outlives the connector: once the cursor is frozen, every retry of the SyncFlow activity must run
// strict, without replaying (and syncing) the same changes again, or the mirror would keep looking like it makes
// progress and never be reported STALLED. A strict batch waits for a resolved timestamp instead, so the mirror
// recovers by itself once one arrives. Kept per flow-worker process: a restart starts counting again.
var frozenCursors sync.Map // flow job name -> *frozenCursorTracker

func frozenCursorFor(flowJobName string) *frozenCursorTracker {
	tracker, _ := frozenCursors.LoadOrStore(flowJobName, &frozenCursorTracker{})
	return tracker.(*frozenCursorTracker)
}

// observe records one finished batch: whether it force-emitted records past the checkpoint, whether it did so after
// holding for a resolved timestamp as long as a batch may (changefeedCatchUpMaxDuration), its start and end cursors,
// and its settings. Any batch that advanced the cursor, ended on a resolved timestamp, or ran with other settings
// resets the tracker. A capped hold counts as frozen at once: it already waited far longer than the replay window
// the batch count stands for, and counting 20 of them would take 20 hours.
func (f *frozenCursorTracker) observe(
	forced bool, capped bool, start, end crdbHLC, settings frozenCursorSettings, now time.Time,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !forced || end != start {
		f.batches = 0
		return
	}
	if f.batches == 0 || f.cursor != start || f.settings != settings {
		f.cursor, f.settings, f.since, f.batches = start, settings, now, 0
	}
	f.batches++
	if capped {
		f.batches = max(f.batches, frozenCursorMinBatches)
		if since := now.Add(-frozenCursorMinDuration); f.since.After(since) {
			f.since = since
		}
	}
}

// frozen reports whether a batch starting at cursor with these settings would replay a cursor that has been stuck
// long enough, over enough replayed batches, to be a clear failure rather than a slow catch-up (e.g. a node outage
// stalling resolved timestamps for a few minutes). It returns the tracked state for the error message.
func (f *frozenCursorTracker) frozen(
	cursor crdbHLC, settings frozenCursorSettings, now time.Time,
) (bool, time.Time, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ok := f.batches >= frozenCursorMinBatches && now.Sub(f.since) >= frozenCursorMinDuration &&
		f.cursor == cursor && f.settings == settings
	return ok, f.since, f.batches
}
