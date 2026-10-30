package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

// A normalize failure must name the table whose statement failed. Normalize sends every table's statement
// in one pgx batch; when one fails to prepare, the error was reported under the batch's first table. Here
// the last of four tables (alphabetically) is broken on the target, so its MERGE can't prepare.
func (s MySQLPGSuite) Test_Normalize_Error_Names_Failing_Table() {
	t := s.t
	tables := []string{"nerr_a", "nerr_b", "nerr_c", "nerr_d"}
	broken := tables[len(tables)-1]
	pairs := make([]string, 0, 2*len(tables))
	for _, table := range tables {
		require.NoError(t, s.source.Exec(t.Context(),
			fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, AttachSchema(s, table))))
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a')`, AttachSchema(s, table))))
		pairs = append(pairs, table, table)
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, "nerr"),
		TableMappings: TableMappings(s, pairs...),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(broken))) == 1
	})

	// break one table on the target: its MERGE references a column that's gone
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s DROP COLUMN v`, s.DestinationTable(broken)))
	EnvNoError(t, env, err)
	for _, table := range tables {
		EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 'b')`, AttachSchema(s, table))))
	}

	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	EnvWaitFor(t, env, 3*time.Minute, "normalize error naming the failing table", func() bool {
		count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "error",
			"normalize statement for table "+s.DestinationTable(broken))
		if err != nil {
			t.Log("error querying flow_errors:", err)
			return false
		}
		return count > 0
	})
	// and never under the batch's first table
	count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "error",
		"normalize statement for table "+s.DestinationTable(tables[0]))
	EnvNoError(t, env, err)
	EnvTrue(t, env, count == 0)
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
