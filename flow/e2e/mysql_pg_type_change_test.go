package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

// A source column's type changed mid-stream (MODIFY COLUMN isn't replicated, so the mirror keeps the old type):
// values the old type can't read must not arrive as made-up numbers. Here INT, BIGINT, DOUBLE and TINYINT(1)
// columns become VARCHAR, then a row holds text in them. Normalize fails on it with an error naming the table
// (invalid input for the target's type), which the classifier reports as user-actionable
// (TestPostgresNormalizeErrorsNeedingTheUser); the row never arrives with other values.
func (s MySQLPGSuite) Test_Type_Change_Values_Not_Coerced() {
	t := s.t
	srcTable := "type_change_coercion"
	srcFull, dstFull := AttachSchema(s, srcTable), s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, n INT, big BIGINT, f DOUBLE, b TINYINT(1))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, 1, 1.5, 1)`, srcFull)))
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
		return s.destInt(`SELECT count(*) FROM `+dstFull) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`ALTER TABLE %s MODIFY n VARCHAR(30), MODIFY big VARCHAR(30), MODIFY f VARCHAR(30), MODIFY b VARCHAR(30)`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (2, 'abc', '99999999999999999999', 'not a number', 'yes please')`, srcFull)))

	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	// GetLogCount pastes the pattern into SQL quotes, so no apostrophes
	EnvWaitFor(t, env, 3*time.Minute, "normalize error naming the table and the invalid input", func() bool {
		named, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "error",
			"error executing normalize statement for table "+dstFull)
		if err != nil || named == 0 {
			return false
		}
		invalid, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "error",
			"invalid input syntax for type")
		return err == nil && invalid > 0
	})
	row2 := s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2`, dstFull))
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	require.Zero(t, row2, "row 2 must not arrive with values its source text doesn't hold")
}
