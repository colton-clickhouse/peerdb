package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Arrays of types pgx doesn't know (enum, domain) arrive as their text form and are parsed into a string slice; an
// empty array '{}' must stay empty, not become NULL, on both paths. Rows 1-3 come from the snapshot, 11-13 from CDC.
func (s PeerFlowE2ETestSuitePG) Test_Empty_Enum_And_Domain_Arrays_PG() {
	t := s.t
	srcTableName := s.attachSchemaSuffix("empty_arrays")
	dstTableName := s.attachSchemaSuffix("empty_arrays_dst")
	schema := "e2e_test_" + s.suffix
	_, err := s.Conn().Exec(t.Context(), fmt.Sprintf(`
		CREATE TYPE %[1]s.mood AS ENUM ('sad', 'ok');
		CREATE DOMAIN %[1]s.label AS text;
		CREATE TABLE %[2]s (id INT PRIMARY KEY, e %[1]s.mood[], l %[1]s.label[]);
		INSERT INTO %[2]s VALUES (1, '{}', '{}'), (2, NULL, NULL), (3, '{ok,sad}', '{a,b}');`, schema, srcTableName))
	require.NoError(t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("empty_arrays"),
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
		`INSERT INTO %s VALUES (11, '{}', '{}'), (12, NULL, NULL), (13, '{ok,sad}', '{a,b}')`, srcTableName))
	EnvNoError(t, env, err)
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool { return count("id > 10") == 3 })
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	for _, id := range []int{1, 2, 3, 11, 12, 13} {
		var got, want string
		q := "SELECT coalesce(e::text, 'NULL') || '|' || coalesce(l::text, 'NULL') FROM %s WHERE id = %d"
		require.NoError(t, s.Conn().QueryRow(t.Context(), fmt.Sprintf(q, srcTableName, id)).Scan(&want))
		require.NoError(t, s.Conn().QueryRow(t.Context(), fmt.Sprintf(q, dstTableName, id)).Scan(&got))
		require.Equal(t, want, got, "id %d", id)
	}
}
