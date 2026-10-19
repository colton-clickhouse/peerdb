package e2e

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"
)

// Unsigned integers (MySQL -> Managed PG Pass 1 findings #5 and #20). On v0.37.10 PeerDB creates each
// unsigned column one size too small (#5: values above the signed max fail the snapshot COPY), and the
// CDC MERGE casts every value to PeerDB's own mapped type rather than the target column's (#20), so even a
// target column widened by hand can't take those values through CDC: normalize fails, the mirror stalls.

// the types PeerDB creates on the destination for each unsigned type
func (s MySQLPGSuite) Test_Unsigned_Target_Types() {
	t := s.t
	srcTable := "unsigned_types"
	srcFull := AttachSchema(s, srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_tiny TINYINT UNSIGNED, c_small SMALLINT UNSIGNED, c_medium MEDIUMINT UNSIGNED, c_int INT UNSIGNED,
		c_big BIGINT UNSIGNED)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, 1, 1, 1, 1)`, srcFull)))

	env := s.startUnsignedFlow(srcTable)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(srcTable))) == 1
	})
	types := s.destColumnTypes(srcTable)
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	t.Log("unsigned target types:", types)
	// every type must hold its source's full range; BIGINT UNSIGNED is a per-migration choice (bigint or
	// numeric(20,0), see Test_Unsigned_CDC_Into_Wider_Target), so it isn't asserted here
	for col, want := range map[string]string{
		"c_tiny": "smallint", "c_small": "integer", "c_medium": "integer", "c_int": "bigint",
	} {
		require.Equal(t, want, types[col], "%s: target type too narrow for the unsigned range (#5)", col)
	}
}

// each unsigned type's maximum value through the snapshot
func (s MySQLPGSuite) Test_Unsigned_Max_Values_Snapshot() {
	t := s.t
	srcTable := "unsigned_max_snap"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_tiny TINYINT UNSIGNED, c_small SMALLINT UNSIGNED, c_medium MEDIUMINT UNSIGNED, c_int INT UNSIGNED)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (1, 255, 65535, 16777215, 4294967295)`, srcFull)))

	env := s.startUnsignedFlow(srcTable)
	EnvWaitFor(t, env, 2*time.Minute, "snapshot of unsigned max values (#5)", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE c_tiny = 255 AND c_small = 65535
			AND c_medium = 16777215 AND c_int = 4294967295`, dstFull)) == 1
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// CDC into target columns wider than PeerDB's mapping: the #5 workaround (INT UNSIGNED -> bigint) and the
// per-migration choice for BIGINT UNSIGNED (numeric(20,0)). Normalize must cast to the target's types.
func (s MySQLPGSuite) Test_Unsigned_CDC_Into_Wider_Target() {
	t := s.t
	srcTable := "unsigned_cdc_wide"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_int INT UNSIGNED, c_big BIGINT UNSIGNED)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, 1)`, srcFull)))

	env := s.startUnsignedFlow(srcTable)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(
		`ALTER TABLE %s ALTER COLUMN c_int TYPE bigint, ALTER COLUMN c_big TYPE numeric(20,0)`, dstFull))
	EnvNoError(t, env, err)

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (2, 4294967295, 18446744073709551615)`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %s SET c_int = 4294967294, c_big = 18446744073709551614 WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "unsigned max values through CDC into wider target columns (#20)", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id, c_int, c_big) IN
			((2, 4294967295, 18446744073709551615), (1, 4294967294, 18446744073709551614))`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s MySQLPGSuite) startUnsignedFlow(srcTable string) WorkflowRun {
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	return env
}

// destColumnTypes returns column -> data_type of the destination table
func (s MySQLPGSuite) destColumnTypes(table string) map[string]string {
	schema, name, _ := strings.Cut(s.DestinationTable(table), ".")
	rows, err := s.dest.Conn().Query(s.t.Context(),
		`SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`,
		schema, name)
	require.NoError(s.t, err)
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var c, ty string
		require.NoError(s.t, rows.Scan(&c, &ty))
		out[c] = ty
	}
	require.NoError(s.t, rows.Err())
	return out
}
