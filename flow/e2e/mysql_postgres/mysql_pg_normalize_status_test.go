//go:build tilt

package mysql_postgres

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
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
			fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, e2e.AttachSchema(s, table))))
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a')`, e2e.AttachSchema(s, table))))
		pairs = append(pairs, table, table)
	}
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, "nerr"),
		TableMappings: e2e.TableMappings(s, pairs...),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t, tc, flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(broken))) == 1
	})

	// break one table on the target: its MERGE references a column that's gone
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s DROP COLUMN v`, s.DestinationTable(broken)))
	e2e.EnvNoError(t, env, err)
	for _, table := range tables {
		e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 'b')`, e2e.AttachSchema(s, table))))
	}

	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "normalize error naming the failing table", func() bool {
		count, err := e2e.GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "error",
			"normalize statement for table "+s.DestinationTable(broken))
		if err != nil {
			t.Log("error querying flow_errors:", err)
			return false
		}
		return count > 0
	})
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}
