package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// A mirror that keeps failing with a user-actionable error, without progress, for longer than
// PEERDB_STALLED_THRESHOLD_MINUTES is reported STALLED instead of RUNNING, and goes back to RUNNING once it
// makes progress. Here a column the mirror writes is dropped from the target, so normalize fails (42703)
// until it's back.
func (s MySQLPGSuite) Test_Stalled_Status_Set_And_Cleared() {
	t := s.t
	srcTable := "stalled"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a')`, srcFull)))

	SetDynamicSetting(t, "PEERDB_STALLED_THRESHOLD_MINUTES", "1")
	client, err := NewApiClient()
	require.NoError(t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	status := func() protos.FlowStatus {
		res, err := client.MirrorStatus(t.Context(), &protos.MirrorStatusRequest{FlowJobName: flowConnConfig.FlowJobName})
		if err != nil {
			t.Log("mirror status:", err)
			return protos.FlowStatus_STATUS_UNKNOWN
		}
		return res.CurrentFlowState
	}

	_, err = s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s DROP COLUMN v`, dstFull))
	EnvNoError(t, env, err)
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 'b')`, srcFull)))
	EnvWaitFor(t, env, 6*time.Minute, "STALLED while normalize keeps failing", func() bool {
		return status() == protos.FlowStatus_STATUS_STALLED
	})
	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "warn", "Mirror is STALLED")
	EnvNoError(t, env, err)
	require.Positive(t, count, "the mirror's log says why it is STALLED")

	_, err = s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s ADD COLUMN v text`, dstFull))
	EnvNoError(t, env, err)
	EnvWaitFor(t, env, 8*time.Minute, "RUNNING again once normalize makes progress", func() bool {
		return status() == protos.FlowStatus_STATUS_RUNNING &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 AND v = 'b'`, dstFull)) == 1
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
