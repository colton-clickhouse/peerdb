package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// catchUpRows is the size of the transaction the catch-up tests commit: large enough that the changefeed needs
// longer than the 1 s sync interval (plus the 2 s grace) to emit it, so no resolved timestamp arrives within a batch.
const catchUpRows = 200_000

// A changefeed only emits a resolved timestamp past a transaction once it has emitted all of its rows, and the same
// holds for a catch-up from an old cursor. When that takes longer than the batch window, the batch used to end with
// the cursor unchanged, and the next batch restarted the changefeed from it and replayed the same rows, forever
// (a backlog over 128 MB, or one that outlasts the sync interval, e.g. after a network partition). The
// mirror looked healthy: batches kept landing, but nothing after the cursor ever arrived.
func (s CRDBPGSuite) Test_Large_Transaction_Catches_Up() {
	t := s.t
	srcTable, dstFull, flowConnConfig, env := s.catchUpMirror("catch_up", shared.InternalVersion_Latest)

	EnvWaitFor(t, env, 5*time.Minute, "the large transaction arrives", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == catchUpRows+1
	})
	// the cursor moved past it: a later change arrives as usual
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (%d, 'after')`, AttachSchema(s, srcTable), catchUpRows+1)))
	EnvWaitFor(t, env, 2*time.Minute, "a change after the transaction", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = %d`, dstFull, catchUpRows+1)) == 1
	})
	t.Log("batches:", s.destText(fmt.Sprintf(
		`SELECT sync_batch_id::text FROM _peerdb_internal.peerdb_mirror_jobs WHERE mirror_job_name = '%s'`,
		flowConnConfig.FlowJobName)))

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Mirrors created before InternalVersion_CockroachHoldBatchForResolved keep ending batches at the deadline, so the
// transaction replays from the same cursor batch after batch.
func (s CRDBPGSuite) Test_Large_Transaction_Old_Version_Replays() {
	t := s.t
	_, dstFull, flowConnConfig, env := s.catchUpMirror("catch_up_old", shared.InternalVersion_CockroachHoldBatchForResolved-1)

	jobs := fmt.Sprintf(`SELECT %%s FROM _peerdb_internal.peerdb_mirror_jobs WHERE mirror_job_name = '%s'`,
		flowConnConfig.FlowJobName)
	EnvWaitFor(t, env, 2*time.Minute, "a batch of the transaction", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) > 1
	})
	cursor, batch := s.destInt(fmt.Sprintf(jobs, "lsn_offset")), s.destInt(fmt.Sprintf(jobs, "sync_batch_id"))
	EnvWaitFor(t, env, 3*time.Minute, "10 more batches", func() bool {
		return s.destInt(fmt.Sprintf(jobs, "sync_batch_id")) >= batch+10
	})
	require.Equal(t, cursor, s.destInt(fmt.Sprintf(jobs, "lsn_offset")), "the cursor is frozen")
	require.Less(t, s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)), int64(catchUpRows+1))

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// catchUpMirror starts a mirror with a 1 s sync interval and 1000-row batches on a one-row table, waits for the
// snapshot, then commits catchUpRows rows in one transaction.
func (s CRDBPGSuite) catchUpMirror(srcTable string, version uint32) (string, string, *protos.FlowConnectionConfigs, WorkflowRun) {
	t := s.t
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, pad STRING)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (0, 'snapshot')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	flowConnConfig.IdleTimeoutSeconds = 1
	flowConnConfig.MaxBatchSize = 1000
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": fmt.Sprint(version)}
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 2*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s SELECT i, repeat('x', 100) FROM generate_series(1, %d) AS i`, srcFull, catchUpRows)))
	return srcTable, dstFull, flowConnConfig, env
}
