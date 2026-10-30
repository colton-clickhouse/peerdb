package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// TIMETZ values whose UTC time of day falls on another day than their own (01:00+05 is 20:00 UTC the day before,
// 23:30-05 is 04:30 UTC the next day), through the snapshot (rows 1-3) and CDC (11-13). The Postgres source's parser
// gave CDC -04:00:00 and 28:30:00, which the normalize cast to timetz rejects, so CDC stopped for the mirror.
func (s PeerFlowE2ETestSuitePG) Test_TimeTZ_Across_Midnight_PG() {
	t := s.t
	srcTableName := s.attachSchemaSuffix("timetz_midnight")
	dstTableName := s.attachSchemaSuffix("timetz_midnight_dst")
	_, err := s.Conn().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %[1]s (id INT PRIMARY KEY, c TIMETZ);
		INSERT INTO %[1]s VALUES (1, '01:00:00+05'), (2, '23:30:00-05'), (3, '12:34:56.789+00');`, srcTableName))
	require.NoError(t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("timetz_midnight"),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""
	flowConnConfig.System = protos.TypeSystem_Q
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	count := func(where string) int {
		var n int
		if err := s.Conn().QueryRow(t.Context(), fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", dstTableName, where)).Scan(&n); err != nil {
			return -1
		}
		return n
	}
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool { return count("true") == 3 })
	_, err = s.Conn().Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (11, '01:00:00+05'), (12, '23:30:00-05'), (13, '12:34:56.789+00')`, srcTableName))
	EnvNoError(t, env, err)
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool { return count("id > 10") == 3 })
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	// PeerDB keeps the UTC instant of day, not the source's offset
	for id, want := range map[int]string{1: "20:00:00+00", 2: "04:30:00+00", 3: "12:34:56.789+00",
		11: "20:00:00+00", 12: "04:30:00+00", 13: "12:34:56.789+00"} {
		var got string
		require.NoError(t, s.Conn().QueryRow(t.Context(),
			fmt.Sprintf("SELECT (c AT TIME ZONE 'UTC')::text FROM %s WHERE id = %d", dstTableName, id)).Scan(&got))
		require.Equal(t, want, got, "id %d", id)
	}
}
