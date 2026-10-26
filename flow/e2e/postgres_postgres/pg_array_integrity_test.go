//go:build tilt

package postgres_postgres

import (
	"fmt"
	"strconv"
	"time"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// arrayIntegrityRows are a snapshot row (1), a CDC insert (2) and a CDC update of row 1, with +-infinity and NULL
// elements in date, timestamp and timestamptz arrays and NULL elements in an int array.
const arrayIntegrityValues = `ARRAY['2024-01-02','infinity','-infinity',NULL]::date[],
	ARRAY['2024-01-02 03:04:05','infinity','-infinity',NULL]::timestamp[],
	ARRAY['2024-01-02 03:04:05+00','infinity','-infinity',NULL]::timestamptz[],
	ARRAY[1,NULL,3]::int8[]`

func (s PeerFlowE2ETestSuitePG) arrayIntegrityMirror(name string, version uint32) (string, string) {
	t := s.t
	srcTableName := s.attachSchemaSuffix(name)
	dstTableName := s.attachSchemaSuffix(name + "_dst")
	_, err := s.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v INT,
		d date[], ts timestamp[], tstz timestamptz[], i int8[])`, srcTableName))
	require.NoError(t, err)
	_, err = s.Conn().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 0, %s)`, srcTableName, arrayIntegrityValues))
	require.NoError(t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(name),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""
	flowConnConfig.System = protos.TypeSystem_Q
	flowConnConfig.DoInitialSnapshot = true
	// mirror creation replaces Version with PEERDB_FORCE_INTERNAL_VERSION (default: latest)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(version), 10)}
	flowConnConfig.Version = version

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t, tc, flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		var n int
		return s.Conn().QueryRow(t.Context(), "SELECT count(*) FROM "+dstTableName).Scan(&n) == nil && n == 1
	})
	_, err = s.Conn().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 0, %s)`, srcTableName, arrayIntegrityValues))
	e2e.EnvNoError(t, env, err)
	_, err = s.Conn().Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 1 WHERE id = 1`, srcTableName))
	e2e.EnvNoError(t, env, err)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		var n int
		return s.Conn().QueryRow(t.Context(), fmt.Sprintf(
			"SELECT count(*) FROM %s WHERE id = 2 OR (id = 1 AND v = 1)", dstTableName)).Scan(&n) == nil && n == 2
	})
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
	return srcTableName, dstTableName
}

// A Postgres source's +-infinity and NULL array elements reach a Postgres destination as infinity and NULL on both
// paths, instead of 0001-01-01 / 0.
func (s PeerFlowE2ETestSuitePG) Test_Array_Infinity_And_Null_Elements_PG() {
	src, dst := s.arrayIntegrityMirror("array_integrity", shared.InternalVersion_Latest)
	require.NoError(s.t, s.comparePGTables(src, dst, "id,d::text,ts::text,tstz::text,i::text"))
}
