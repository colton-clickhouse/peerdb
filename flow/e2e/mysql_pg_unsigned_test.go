package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Unsigned integers to Postgres: each type's maximum value through the snapshot, into a target column wide
// enough for its range. BIGINT UNSIGNED stays bigint (identity-compatible); numeric(20,0) is the user's
// choice, see Test_Unsigned_CDC_Into_Wider_Target.
func (s MySQLPGSuite) Test_Unsigned_Max_Values_Snapshot() {
	t := s.t
	srcTable := "unsigned_max_snap"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_tiny TINYINT UNSIGNED, c_small SMALLINT UNSIGNED, c_medium MEDIUMINT UNSIGNED, c_int INT UNSIGNED,
		c_big BIGINT UNSIGNED)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (1, 255, 65535, 16777215, 4294967295, 9223372036854775807)`, srcFull)))

	env := s.startUnsignedFlow(srcTable, shared.InternalVersion_Latest)
	EnvWaitFor(t, env, 2*time.Minute, "snapshot of unsigned max values", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE c_tiny = 255 AND c_small = 65535
			AND c_medium = 16777215 AND c_int = 4294967295 AND c_big = 9223372036854775807`, dstFull)) == 1
	})
	types := s.destColumnTypes(srcTable)
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	// MEDIUMINT UNSIGNED is UInt32 inside PeerDB, so bigint
	for col, want := range map[string]string{
		"c_tiny": "smallint", "c_small": "integer", "c_medium": "bigint", "c_int": "bigint", "c_big": "bigint",
	} {
		require.Equal(t, want, types[col], col)
	}
}

// CDC into target columns other than PeerDB's mapping: INT UNSIGNED widened to bigint, BIGINT UNSIGNED as
// numeric(20,0) (the per-migration choice), and a CHAR(5) as char(5). Normalize must cast to the target's
// types, and not truncate.
func (s MySQLPGSuite) Test_Unsigned_CDC_Into_Wider_Target() {
	t := s.t
	srcTable := "unsigned_cdc_wide"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_int INT UNSIGNED, c_big BIGINT UNSIGNED, c_code CHAR(5))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, 1, 'a')`, srcFull)))

	env := s.startUnsignedFlow(srcTable, shared.InternalVersion_Latest)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN c_int TYPE bigint,
		ALTER COLUMN c_big TYPE numeric(20,0), ALTER COLUMN c_code TYPE char(5)`, dstFull))
	EnvNoError(t, env, err)

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (2, 4294967295, 18446744073709551615, 'abcde')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %s SET c_int = 4294967294, c_big = 18446744073709551614, c_code = 'vwxyz' WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "unsigned max values and full CHAR values through CDC", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id, c_int, c_big, c_code) IN
			((2, 4294967295, 18446744073709551615, 'abcde'), (1, 4294967294, 18446744073709551614, 'vwxyz'))`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// startUnsignedFlow starts a mirror of srcTable at the given internal version
func (s MySQLPGSuite) startUnsignedFlow(srcTable string, version uint32) WorkflowRun {
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	// mirror creation replaces Version with PEERDB_FORCE_INTERNAL_VERSION (default: latest)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(version), 10)}
	flowConnConfig.Version = version
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

// mirrors created before InternalVersion_PgWidenUnsignedInts keep their narrower types, so an existing
// mirror's resync or new tables don't change type (their casts: TestGenerateMergeStatement_TargetColumnTypes)
func (s MySQLPGSuite) Test_Unsigned_Old_Version_Keeps_Types() {
	t := s.t
	srcTable := "unsigned_types_old"
	srcFull := AttachSchema(s, srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_small SMALLINT UNSIGNED, c_int INT UNSIGNED)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, 1)`, srcFull)))

	env := s.startUnsignedFlow(srcTable, shared.InternalVersion_PgWidenUnsignedInts-1)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(srcTable))) == 1
	})
	types := s.destColumnTypes(srcTable)
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, "smallint", types["c_small"])
	require.Equal(t, "integer", types["c_int"])
}
