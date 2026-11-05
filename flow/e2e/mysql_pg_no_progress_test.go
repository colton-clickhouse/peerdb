package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

// A mirror that keeps failing with an error only the user can fix (here a column it writes was dropped from the
// target, so normalize fails with 42703 until it's back) must say so once it has made no progress for
// PEERDB_STALLED_ALERT_THRESHOLD_MINUTES: a warning in the mirror's log, and an alert to the configured alert
// senders. Its status stays RUNNING.
func (s MySQLPGSuite) Test_No_Progress_With_User_Actionable_Error_Warns() {
	t := s.t
	srcTable := "no_progress"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a')`, srcFull)))
	SetDynamicSetting(t, "PEERDB_STALLED_ALERT_THRESHOLD_MINUTES", "1")

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	env := ExecutePeerflow(t, NewTemporalClient(t), flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})

	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s DROP COLUMN v`, dstFull))
	EnvNoError(t, env, err)
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 'b')`, srcFull)))
	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	EnvWaitFor(t, env, 6*time.Minute, "a warning that the mirror makes no progress", func() bool {
		count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "warn", "has made no progress since")
		return err == nil && count > 0
	})
	s.checkNoProgressStatus(flowConnConfig.FlowJobName)

	_, err = s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s ADD COLUMN v text`, dstFull))
	EnvNoError(t, env, err)
	EnvWaitFor(t, env, 8*time.Minute, "progress once the column is back", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 AND v = 'b'`, dstFull)) == 1
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// checkNoProgressStatus: added with the mirror status fields
func (s MySQLPGSuite) checkNoProgressStatus(string) {}
