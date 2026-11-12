package e2e

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// A batch synced into the raw table whose normalize didn't finish before SyncFlow ended (here: normalize blocked on
// a lock, then the mirror paused) used to wait for the next sync, so with no new source rows it stayed out of the
// destination table while the mirror reported RUNNING. SyncFlow now requests it when it starts.
func (s CRDBPGSuite) Test_Pending_Normalize_After_Restart() {
	t := s.t
	srcTable := "pending_normalize"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, v STRING)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'snapshot')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})

	// hold a lock that normalize's MERGE into the destination table waits on
	lockConn, err := pgx.Connect(t.Context(), internal.GetPGConnectionString(internal.GetAncillaryPostgresConfigFromEnv(), ""))
	require.NoError(t, err)
	defer lockConn.Close(t.Context())
	lockTx, err := lockConn.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = lockTx.Rollback(t.Context()) }()
	_, err = lockTx.Exec(t.Context(), fmt.Sprintf(`LOCK TABLE %s IN ACCESS EXCLUSIVE MODE`, dstFull))
	require.NoError(t, err)

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 'cdc')`, srcFull)))
	rawTable := "_peerdb_internal._peerdb_raw_" + strings.ToLower(shared.ReplaceIllegalCharactersWithUnderscores(
		flowConnConfig.FlowJobName))
	EnvWaitFor(t, env, 3*time.Minute, "row 2 synced to the raw table", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE _peerdb_data->>'id' = '2'`, rawTable)) == 1
	})

	SignalWorkflow(t.Context(), env, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(t, env, 4*time.Minute, "paused", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})
	require.NoError(t, lockTx.Rollback(t.Context()))
	require.EqualValues(t, 0, s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2`, dstFull)),
		"row 2 was normalized before the pause; the test didn't strand the batch")

	// no new source rows from here on
	SignalWorkflow(t.Context(), env, model.FlowSignal, model.NoopSignal)
	EnvWaitFor(t, env, 2*time.Minute, "running again", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING
	})
	EnvWaitFor(t, env, 3*time.Minute, "row 2 normalized without a new sync", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 AND v = 'cdc'`, dstFull)) == 1
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
