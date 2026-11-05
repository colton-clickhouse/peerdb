package e2e

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"
)

// CDC into target columns other than PeerDB's mapping (pre-created or altered by the user): normalize casts to
// the target's own types, as the snapshot COPY already does.

// INT UNSIGNED widened to bigint, BIGINT UNSIGNED as numeric(20,0), and a CHAR(5) as char(5): the values must
// arrive whole and untruncated.
func (s MySQLPGSuite) Test_Unsigned_CDC_Into_Wider_Target() {
	t := s.t
	srcTable := "unsigned_cdc_wide"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_int INT UNSIGNED, c_big BIGINT UNSIGNED, c_code CHAR(5))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, 1, 'a')`, srcFull)))

	env := s.startTargetTypesFlow(srcTable)
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

// DECIMAL and DOUBLE into integer targets. Their text form ('5.00') isn't integer input, so a direct cast to the
// target's type fails normalize; numeric and float convert to integers by rounding, as they did by assignment.
func (s MySQLPGSuite) Test_Decimal_CDC_Into_Integer_Target() {
	t := s.t
	srcTable := "decimal_cdc_int"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_dec DECIMAL(10,2), c_dbl DOUBLE)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, 1)`, srcFull)))

	env := s.startTargetTypesFlow(srcTable)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN c_dec TYPE integer,
		ALTER COLUMN c_dbl TYPE bigint`, dstFull))
	EnvNoError(t, env, err)

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (2, 5.00, 7), (3, 6.50, 1e15), (4, 2.50, 6.5)`, srcFull)))
	// rounding as Postgres converts: numeric half away from zero, double precision half to even
	EnvWaitFor(t, env, 2*time.Minute, "decimal and double values in integer columns through CDC", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id, c_dec, c_dbl) IN
			((2, 5, 7), (3, 7, 1000000000000000), (4, 3, 6))`, dstFull)) == 3
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// MySQL ENUM into a Postgres enum target. Text has no assignment cast to an enum, so normalize failed on it.
func (s MySQLPGSuite) Test_Enum_CDC_Into_Enum_Target() {
	t := s.t
	srcTable := "enum_cdc_enum"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	dstSchema, _, _ := strings.Cut(dstFull, ".")
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, mood ENUM('sad', 'happy'))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'sad')`, srcFull)))

	env := s.startTargetTypesFlow(srcTable)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TYPE %[1]s.mood AS ENUM ('sad', 'happy');
		ALTER TABLE %[2]s ALTER COLUMN mood TYPE %[1]s.mood USING mood::%[1]s.mood`, dstSchema, dstFull))
	EnvNoError(t, env, err)

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 'happy')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET mood = 'happy' WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "enum values through CDC", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE mood = 'happy'`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Regression guard for casting to the target's types: a target column changed to text or varchar must hold
// exactly the text it held when values were cast to PeerDB's type and assigned, for every MySQL kind. Two
// identical source tables in one mirror; one target keeps PeerDB's types, the other's columns become text after
// the snapshot, and every value of the text table must equal the other's cast to text.
func (s MySQLPGSuite) Test_CDC_Into_Text_Targets_Keeps_Values() {
	t := s.t
	const cols = `c_int INT, c_big BIGINT, c_uint INT UNSIGNED, c_dec DECIMAL(12,3), c_dbl DOUBLE, c_flt FLOAT,
		c_dt DATETIME(6), c_ts TIMESTAMP(6) NULL, c_date DATE, c_time TIME(3), c_year YEAR, c_bit BIT(8), c_json JSON,
		c_enum ENUM('sad', 'happy'), c_set SET('x', 'y'), c_bin VARBINARY(8), c_str VARCHAR(20)`
	colNames := []string{"c_int", "c_big", "c_uint", "c_dec", "c_dbl", "c_flt", "c_dt", "c_ts", "c_date", "c_time",
		"c_year", "c_bit", "c_json", "c_enum", "c_set", "c_bin", "c_str"}
	const row1 = `(1, -5, 9007199254740993, 2147483647, 1234.500, 0.1, 0.1, '2024-01-02 03:04:05.123456',
		'2024-01-02 03:04:05.5', '2024-02-29', '12:34:56.789', 2024, b'10100101', '{"b": [1, 2.50], "a": "x"}',
		'happy', 'x,y', x'00FF41', 'Ünicode ''q''')`
	const row2 = `(2, 0, -1, 0, -0.001, 1e-7, 3.4e38, '1000-01-01 00:00:00', NULL, '9999-12-31', '23:59:59.999', 1901,
		b'0', '[]', 'sad', '', x'', '')`
	tables := []string{"text_targets_default", "text_targets_text"}
	for _, table := range tables {
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, %s)`,
			AttachSchema(s, table), cols)))
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES %s`, AttachSchema(s, table), row1)))
	}
	dflt, text := s.DestinationTable(tables[0]), s.DestinationTable(tables[1])

	env := s.startTargetTypesFlow(tables...)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT (SELECT count(*) FROM %s) + (SELECT count(*) FROM %s)`, dflt, text)) == 2
	})
	alters := make([]string, 0, len(colNames))
	for _, c := range colNames {
		target := "text"
		if c == "c_str" {
			target = "varchar(40)"
		}
		alters = append(alters, fmt.Sprintf("ALTER COLUMN %[1]s TYPE %[2]s USING %[1]s::%[2]s", c, target))
	}
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s %s`, text, strings.Join(alters, ", ")))
	EnvNoError(t, env, err)

	for _, table := range tables {
		srcFull := AttachSchema(s, table)
		EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES %s, (3, %s)`,
			srcFull, row2, strings.TrimSuffix(strings.Repeat("NULL, ", len(colNames)), ", "))))
		EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES %s`,
			srcFull, strings.Replace(row1, "(1,", "(4,", 1))))
		EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
			`UPDATE %s SET c_int = c_int + 1, c_dt = '2025-06-07 08:09:10.000001', c_json = '{"z": null}' WHERE id = 1`,
			srcFull)))
	}
	EnvWaitFor(t, env, 2*time.Minute, "CDC into both tables", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE c_int = '-4'`, text)) == 1 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, text)) == 4 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE c_int = -4`, dflt)) == 1 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dflt)) == 4
	})
	var differ []string
	for _, c := range colNames {
		var ids []int32
		rows, err := s.dest.Conn().Query(t.Context(), fmt.Sprintf(`SELECT d.id FROM %[1]s d JOIN %[2]s x USING (id)
			WHERE x.%[3]s IS DISTINCT FROM d.%[3]s::text ORDER BY 1`, dflt, text, c))
		EnvNoError(t, env, err)
		for rows.Next() {
			var id int32
			EnvNoError(t, env, rows.Scan(&id))
			ids = append(ids, id)
		}
		EnvNoError(t, env, rows.Err())
		if len(ids) > 0 {
			differ = append(differ, fmt.Sprintf("%s (rows %v)", c, ids))
		}
	}
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	require.Empty(t, differ, "text-target values that differ from PeerDB's type cast to text")
}

// startTargetTypesFlow starts a mirror of the given tables, each into its own destination table
func (s MySQLPGSuite) startTargetTypesFlow(srcTables ...string) WorkflowRun {
	pairs := make([]string, 0, 2*len(srcTables))
	for _, table := range srcTables {
		pairs = append(pairs, table, table)
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTables[0]),
		TableMappings: TableMappings(s, pairs...),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	return env
}
