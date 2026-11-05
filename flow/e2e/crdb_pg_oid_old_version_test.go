package e2e

import (
	"fmt"
	"strconv"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// A mirror from before InternalVersion_PgWidenUnsignedInts has an integer target column for OID, and its normalize
// still casts to it: an OID of 2^31 or more fails there (22003), which is user-actionable, so the mirror goes
// STATUS_STALLED instead of retrying silently. Widening the column doesn't help such a mirror (its normalize casts to
// integer whatever the column is); recreating it does.
func (s CRDBPGSuite) Test_OID_Above_Int32_Max_Old_Version_Stalls() {
	t := s.t
	srcTable := "oid_big_old"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	client, err := NewApiClient()
	require.NoError(t, err)
	SetDynamicSetting(t, "PEERDB_STALLED_THRESHOLD_MINUTES", "1")

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, o OID)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1)`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	version := shared.InternalVersion_PgWidenUnsignedInts - 1
	// mirror creation replaces Version with PEERDB_FORCE_INTERNAL_VERSION (default: latest)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(version), 10)}
	flowConnConfig.Version = version
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	require.Equal(t, "integer", s.destText(fmt.Sprintf(`SELECT format_type(atttypid, atttypmod) FROM pg_attribute
		WHERE attrelid = '%s'::regclass AND attname = 'o'`, dstFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 3000000000)`, srcFull)))
	EnvWaitFor(t, env, 6*time.Minute, "STALLED while normalize fails", func() bool {
		res, err := client.MirrorStatus(t.Context(), &protos.MirrorStatusRequest{FlowJobName: flowConnConfig.FlowJobName})
		if err != nil {
			t.Log("mirror status:", err)
			return false
		}
		return res.CurrentFlowState == protos.FlowStatus_STATUS_STALLED
	})
	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "warn",
		"Mirror is STALLED%NOTIFY_INVALID_DESTINATION_TABLE_DEFINITION")
	EnvNoError(t, env, err)
	require.Positive(t, count, "the mirror's log says why it is STALLED")
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
