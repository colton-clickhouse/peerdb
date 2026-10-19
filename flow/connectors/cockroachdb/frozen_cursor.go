package conncockroachdb

import "time"

// A batch that ends without a covering resolved timestamp force-emits its buffer and leaves the stored cursor where
// it was, so the next batch restarts the changefeed from the same cursor and replays. When the replay itself
// outlasts the batch window that repeats forever: rows keep landing, the mirror looks healthy, and nothing after
// the cursor ever arrives (finding #31, the deadline variant of #22). frozenCursorTracker spots that pattern across
// the batches of one SyncFlow activity, so PullRecords can fail the mirror instead.
const (
	frozenCursorMinBatches  = 20
	frozenCursorMinDuration = 15 * time.Minute
)

type frozenCursorTracker struct {
	cursor  crdbHLC
	since   time.Time
	batches int
}

// observe records one finished batch: whether it force-emitted records past the checkpoint, and its start and end
// cursors. Any batch that advanced the cursor, or ended on a resolved timestamp, resets the tracker.
func (f *frozenCursorTracker) observe(forced bool, start, end crdbHLC, now time.Time) {
	if !forced || end != start {
		*f = frozenCursorTracker{}
		return
	}
	if f.batches == 0 || f.cursor != start {
		*f = frozenCursorTracker{cursor: start, since: now}
	}
	f.batches++
}

// frozen reports whether the cursor has been stuck long enough, over enough replayed batches, to be a clear failure
// rather than a slow catch-up (e.g. a node outage stalling resolved timestamps for a few minutes).
func (f *frozenCursorTracker) frozen(now time.Time) bool {
	return f.batches >= frozenCursorMinBatches && now.Sub(f.since) >= frozenCursorMinDuration
}
