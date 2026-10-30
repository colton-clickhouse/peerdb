package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// Columns added mid-stream must get the types a new mirror creates them with: an INT UNSIGNED added later must
// be bigint (4294967295 doesn't fit integer, and normalize would fail on it for the whole mirror), and a TIME
// added later interval (a duration time can't hold).
func (s MySQLPGSuite) Test_Added_Columns_Typed_As_At_Creation() {
	t := s.t
	srcTable := "added_columns"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1)`, srcFull)))

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

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN c_uint INT UNSIGNED, ADD COLUMN c_usmall SMALLINT UNSIGNED, ADD COLUMN c_time TIME`,
		srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (2, 4294967295, 65535, '-838:59:59')`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "added columns", func() bool {
		_, ok := s.destColumnTypes(srcTable)["c_time"]
		return ok
	})
	types := s.destColumnTypes(srcTable)
	if types["c_uint"] != "bigint" || types["c_usmall"] != "integer" || types["c_time"] != "interval" {
		env.Cancel(t.Context())
		RequireEnvCanceled(t, env)
		require.Failf(t, "added columns not typed as at creation", "%v", types)
	}
	EnvWaitFor(t, env, 2*time.Minute, "cdc insert into the added columns", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 AND c_uint = 4294967295
			AND c_usmall = 65535 AND c_time = '-838:59:59'`, dstFull)) == 1
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
