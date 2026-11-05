//go:build tilt

package postgres_postgres

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// An array primary key from Postgres, through the default (Q) type system: normalize's key cast of the raw JSON text
// failed on the JSON array, as for every source. Rows 1 and 2 come from the snapshot; 1 is updated and 2 deleted, and
// 3 inserted, through CDC.
func (s PeerFlowE2ETestSuitePG) Test_Array_Primary_Key_PG() {
	t := s.t
	srcTableName := s.attachSchemaSuffix("array_pk")
	dstTableName := s.attachSchemaSuffix("array_pk_dst")
	_, err := s.Conn().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %[1]s (a int[] PRIMARY KEY, n int);
		INSERT INTO %[1]s VALUES ('{1,2}', 0), ('{3}', 0);`, srcTableName))
	require.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("array_pk"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""
	flowConnConfig.System = protos.TypeSystem_Q
	flowConnConfig.DoInitialSnapshot = true
	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t, tc, flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	rows := func() string {
		var got string
		if err := s.Conn().QueryRow(t.Context(), fmt.Sprintf(
			`SELECT coalesce(string_agg(a::text || '=' || n, ',' ORDER BY a), '') FROM %s`, dstTableName)).Scan(&got); err != nil {
			return err.Error()
		}
		return got
	}
	e2e.EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool { return rows() == "{1,2}=0,{3}=0" })
	_, err = s.Conn().Exec(t.Context(), fmt.Sprintf(`UPDATE %[1]s SET n = 1 WHERE a = '{1,2}';
		DELETE FROM %[1]s WHERE a = '{3}'; INSERT INTO %[1]s VALUES ('{4,5}', 2);`, srcTableName))
	e2e.EnvNoError(t, env, err)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "cdc update, delete and insert", func() bool { return rows() == "{1,2}=1,{4,5}=2" })
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}
