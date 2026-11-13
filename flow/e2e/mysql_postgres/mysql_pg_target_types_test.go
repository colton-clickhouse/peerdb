//go:build tilt

package mysql_postgres

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// CDC into target columns other than PeerDB's mapping (pre-created or altered by the user): normalize casts to
// the target's own types, as the snapshot COPY already does.

// INT UNSIGNED widened to bigint, BIGINT UNSIGNED as numeric(20,0), and a CHAR(5) as char(5): the values must
// arrive whole and untruncated.
func (s MySQLPGSuite) Test_Unsigned_CDC_Into_Wider_Target() {
	t := s.t
	srcTable := "unsigned_cdc_wide"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_int INT UNSIGNED, c_big BIGINT UNSIGNED, c_code CHAR(5))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, 1, 'a')`, srcFull)))

	env := s.startTargetTypesFlow(srcTable)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN c_int TYPE bigint,
		ALTER COLUMN c_big TYPE numeric(20,0), ALTER COLUMN c_code TYPE char(5)`, dstFull))
	e2e.EnvNoError(t, env, err)

	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (2, 4294967295, 18446744073709551615, 'abcde')`, srcFull)))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %s SET c_int = 4294967294, c_big = 18446744073709551614, c_code = 'vwxyz' WHERE id = 1`, srcFull)))
	e2e.EnvWaitFor(t, env, 2*time.Minute, "unsigned max values and full CHAR values through CDC", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id, c_int, c_big, c_code) IN
			((2, 4294967295, 18446744073709551615, 'abcde'), (1, 4294967294, 18446744073709551614, 'vwxyz'))`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

// DECIMAL and DOUBLE into integer targets. Their text form ('5.00') isn't integer input, so a direct cast to the
// target's type fails normalize; numeric and float convert to integers by rounding, as they did by assignment.
func (s MySQLPGSuite) Test_Decimal_CDC_Into_Integer_Target() {
	t := s.t
	srcTable := "decimal_cdc_int"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_dec DECIMAL(10,2), c_dbl DOUBLE)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, 1)`, srcFull)))

	env := s.startTargetTypesFlow(srcTable)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN c_dec TYPE integer,
		ALTER COLUMN c_dbl TYPE bigint`, dstFull))
	e2e.EnvNoError(t, env, err)

	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (2, 5.00, 7), (3, 6.50, 1e15), (4, 2.50, 6.5)`, srcFull)))
	// rounding as Postgres converts: numeric half away from zero, double precision half to even
	e2e.EnvWaitFor(t, env, 2*time.Minute, "decimal and double values in integer columns through CDC", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id, c_dec, c_dbl) IN
			((2, 5, 7), (3, 7, 1000000000000000), (4, 3, 6))`, dstFull)) == 3
	})
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

// MySQL ENUM into a Postgres enum target. Text has no assignment cast to an enum, so normalize failed on it.
func (s MySQLPGSuite) Test_Enum_CDC_Into_Enum_Target() {
	t := s.t
	srcTable := "enum_cdc_enum"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	dstSchema, _, _ := strings.Cut(dstFull, ".")
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, mood ENUM('sad', 'happy'))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'sad')`, srcFull)))

	env := s.startTargetTypesFlow(srcTable)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TYPE %[1]s.mood AS ENUM ('sad', 'happy');
		ALTER TABLE %[2]s ALTER COLUMN mood TYPE %[1]s.mood USING mood::%[1]s.mood`, dstSchema, dstFull))
	e2e.EnvNoError(t, env, err)

	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 'happy')`, srcFull)))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET mood = 'happy' WHERE id = 1`, srcFull)))
	e2e.EnvWaitFor(t, env, 2*time.Minute, "enum values through CDC", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE mood = 'happy'`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

// Regression guard for casting to the target's types: CDC into a text or varchar target must write the text the
// snapshot COPY writes into it. The target is pre-created with text columns, rows are copied by the snapshot and
// the same rows again by CDC, and each CDC row must equal its snapshot row. Only kinds the snapshot can copy into
// text: pgx has no text encoding for Go integers, floats and booleans, so the snapshot fails on those (see
// Test_CDC_Into_Altered_Text_Targets).
//
// Date and time kinds differ, before and after casting to the target's types: the snapshot writes Go's text for
// them ('2024-02-29 00:00:00 +0000 UTC', '12:34:56.789000'), CDC Postgres's ('2024-02-29', '12:34:56.789').
// Pinned here so a change to either side shows.
func (s MySQLPGSuite) Test_CDC_Into_Text_Targets_Keeps_Values() {
	t := s.t
	srcTable := "text_targets_snapshot"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	equalCols := []string{"c_dec", "c_json", "c_enum", "c_set", "c_str"}
	timeCols := []string{"c_dt", "c_ts", "c_date", "c_time"}
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, c_dec DECIMAL(12,3),
		c_json JSON, c_enum ENUM('sad', 'happy'), c_set SET('x', 'y'), c_str VARCHAR(20),
		c_dt DATETIME(6), c_ts TIMESTAMP(6) NULL, c_date DATE, c_time TIME(3))`, srcFull)))
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id integer PRIMARY KEY, c_dec text,
		c_json text, c_enum text, c_set text, c_str varchar(40), c_dt text, c_ts text, c_date text, c_time text,
		"_PEERDB_SYNCED_AT" timestamp DEFAULT CURRENT_TIMESTAMP)`, dstFull))
	require.NoError(t, err)
	rows := map[int]string{
		1: `1234.500, '{"b": [1, 2.50], "a": "x"}', 'happy', 'x,y', 'Ünicode ''q''',
			'2024-01-02 03:04:05.123456', '2024-01-02 03:04:05.5', '2024-02-29', '12:34:56.789'`,
		2: `-0.001, '[]', 'sad', '', '', '1000-01-01 00:00:00', NULL, '9999-12-31', '23:59:59.999'`,
		3: strings.TrimSuffix(strings.Repeat("NULL, ", len(equalCols)+len(timeCols)), ", "),
	}
	insert := func(id, row int) error {
		return s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (%d, %s)`, srcFull, id, rows[row]))
	}
	for id := 1; id <= 3; id++ {
		require.NoError(t, insert(id, id))
	}

	env := s.startTargetTypesFlow(srcTable)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 3
	})
	// CDC row -> snapshot row with the same values: inserts, and an update of an all-NULL row
	pairs := map[int]int{11: 1, 12: 2, 13: 3, 14: 1}
	for id := 11; id <= 13; id++ {
		e2e.EnvNoError(t, env, insert(id, id-10))
	}
	e2e.EnvNoError(t, env, insert(14, 3))
	sets := make([]string, 0, len(equalCols)+len(timeCols))
	for _, c := range slices.Concat(equalCols, timeCols) {
		sets = append(sets, fmt.Sprintf("a.%[1]s = b.%[1]s", c))
	}
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %[1]s a JOIN %[1]s b ON b.id = 1 SET %[2]s
		WHERE a.id = 14`, srcFull, strings.Join(sets, ", "))))
	e2e.EnvWaitFor(t, env, 2*time.Minute, "CDC rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 13 OR (id > 10 AND c_enum IS NOT NULL)`,
			dstFull)) == 4
	})

	var differ []string
	for _, c := range equalCols {
		for cdcID, snapID := range pairs {
			if s.destInt(fmt.Sprintf(`SELECT count(*) FROM %[1]s x JOIN %[1]s y ON y.id = %[3]d
				WHERE x.id = %[2]d AND x.%[4]s IS NOT DISTINCT FROM y.%[4]s`, dstFull, cdcID, snapID, c)) != 1 {
				differ = append(differ, fmt.Sprintf("%s (row %d vs snapshot row %d)", c, cdcID, snapID))
			}
		}
	}
	var snap, cdc [4]*string
	e2e.EnvNoError(t, env, s.dest.Conn().QueryRow(t.Context(), fmt.Sprintf(
		`SELECT c_dt, c_ts, c_date, c_time FROM %s WHERE id = 1`, dstFull)).Scan(&snap[0], &snap[1], &snap[2], &snap[3]))
	e2e.EnvNoError(t, env, s.dest.Conn().QueryRow(t.Context(), fmt.Sprintf(
		`SELECT c_dt, c_ts, c_date, c_time FROM %s WHERE id = 11`, dstFull)).Scan(&cdc[0], &cdc[1], &cdc[2], &cdc[3]))
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
	require.Empty(t, differ, "CDC values in text targets that differ from the snapshot's")
	require.Equal(t, int64(3), s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id IN (1, 11, 14) AND c_dec = '1234.5'`,
		dstFull)), "DECIMAL(12,3) as the snapshot writes it, without the trailing zeros of its scale")
	for _, c := range timeCols {
		require.Equal(t, int64(2), s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id IN (3, 13) AND %s IS NULL`,
			dstFull, c)), c)
	}
	deref := func(v [4]*string) []string {
		out := make([]string, 0, len(v))
		for _, p := range v {
			if p == nil {
				out = append(out, "<NULL>")
			} else {
				out = append(out, *p)
			}
		}
		return out
	}
	require.Equal(t, []string{"2024-01-02 03:04:05.123456 +0000 UTC", "2024-01-02 03:04:05.5 +0000 UTC",
		"2024-02-29 00:00:00 +0000 UTC", "12:34:56.789000"}, deref(snap), "snapshot")
	require.Equal(t, []string{"2024-01-02 03:04:05.123456", "2024-01-02 03:04:05.5", "2024-02-29", "12:34:56.789"},
		deref(cdc), "CDC")
}

// The kinds the snapshot can't copy into text (integers, floats, YEAR, BIT; VARBINARY fails on a NUL byte and
// otherwise writes the raw bytes as text, where CDC writes '\x...'): their target columns become text after the
// snapshot, and each CDC value must be what assignment from PeerDB's own type wrote before, i.e. the same column
// of an identical table that keeps PeerDB's types, cast to text (none of these kinds has a type modifier).
func (s MySQLPGSuite) Test_CDC_Into_Altered_Text_Targets() {
	t := s.t
	const cols = `c_int INT, c_big BIGINT, c_uint INT UNSIGNED, c_dbl DOUBLE, c_flt FLOAT, c_year YEAR, c_bit BIT(8),
		c_bin VARBINARY(8)`
	colNames := []string{"c_int", "c_big", "c_uint", "c_dbl", "c_flt", "c_year", "c_bit", "c_bin"}
	const row1 = `(1, -5, 9007199254740993, 2147483647, 0.1, 0.1, 2024, b'10100101', x'00FF41')`
	const row2 = `(2, 0, -1, 0, 1e-7, 3.4e38, 1901, b'0', x'')`
	tables := []string{"text_targets_default", "text_targets_text"}
	for _, table := range tables {
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, %s)`,
			e2e.AttachSchema(s, table), cols)))
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES %s`, e2e.AttachSchema(s, table), row1)))
	}
	dflt, text := s.DestinationTable(tables[0]), s.DestinationTable(tables[1])

	env := s.startTargetTypesFlow(tables...)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT (SELECT count(*) FROM %s) + (SELECT count(*) FROM %s)`, dflt, text)) == 2
	})
	alters := make([]string, 0, len(colNames))
	for _, c := range colNames {
		alters = append(alters, fmt.Sprintf("ALTER COLUMN %[1]s TYPE text USING %[1]s::text", c))
	}
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s %s`, text, strings.Join(alters, ", ")))
	e2e.EnvNoError(t, env, err)

	for _, table := range tables {
		srcFull := e2e.AttachSchema(s, table)
		e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES %s, (3, %s)`,
			srcFull, row2, strings.TrimSuffix(strings.Repeat("NULL, ", len(colNames)), ", "))))
		e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES %s`,
			srcFull, strings.Replace(row1, "(1,", "(4,", 1))))
		e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET c_int = c_int + 1 WHERE id = 1`, srcFull)))
	}
	e2e.EnvWaitFor(t, env, 2*time.Minute, "CDC into both tables", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE c_int = '-4'`, text)) == 1 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, text)) == 4 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE c_int = -4`, dflt)) == 1 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dflt)) == 4
	})
	var differ []string
	for _, c := range colNames {
		if n := s.destInt(fmt.Sprintf(`SELECT count(*) FROM %[1]s d JOIN %[2]s x USING (id)
			WHERE x.%[3]s IS DISTINCT FROM d.%[3]s::text`, dflt, text, c)); n > 0 {
			differ = append(differ, fmt.Sprintf("%s (%d rows)", c, n))
		}
	}
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
	require.Empty(t, differ, "text-target values that differ from PeerDB's type cast to text")
}

// A domain over varchar(5) target: normalize casts to its base type and leaves the length to assignment, so a
// value that doesn't fit fails normalize, as for a varchar(5) column, instead of arriving truncated (a cast to
// the domain truncates).
func (s MySQLPGSuite) Test_CDC_Into_Domain_Target_Refuses_Long_Value() {
	t := s.t
	srcTable := "domain_cdc_varchar"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	dstSchema, _, _ := strings.Cut(dstFull, ".")
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, code VARCHAR(20))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'abc')`, srcFull)))

	env := s.startTargetTypesFlow(srcTable)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE DOMAIN %[1]s.code5 AS varchar(5);
		CREATE DOMAIN %[1]s.code5_upper AS %[1]s.code5 CHECK (VALUE = upper(VALUE));
		ALTER TABLE %[2]s ALTER COLUMN code TYPE %[1]s.code5_upper USING upper(code)`, dstSchema, dstFull))
	e2e.EnvNoError(t, env, err)

	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 'XYZ')`, srcFull)))
	e2e.EnvWaitFor(t, env, 2*time.Minute, "a fitting value through CDC", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 AND code = 'XYZ'`, dstFull)) == 1
	})
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (3, 'ABCDEFG')`, srcFull)))
	catalog, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	e2e.EnvNoError(t, env, err)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "normalize refusing the over-long value", func() bool {
		n, err := e2e.GetLogCount(t.Context(), catalog, e2e.AddSuffix(s, srcTable), "error",
			"value too long for type character varying(5)")
		return err == nil && n > 0
	})
	require.Zero(t, s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 3`, dstFull)), "over-long value stored")
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

// A string key into a bytea target fails normalize, as before casts followed the target: text has no assignment
// cast to bytea, and an explicit cast would read the text's backslash escapes ('\x41' as the byte 'A'). The
// value keeps PeerDB's type, and so does the key in the batch's PARTITION BY.
func (s MySQLPGSuite) Test_CDC_String_Key_Into_Bytea_Target_Fails() {
	t := s.t
	srcTable := "string_key_bytea"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (code VARCHAR(20) PRIMARY KEY, n INT)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES ('a', 1)`, srcFull)))

	env := s.startTargetTypesFlow(srcTable)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(
		`ALTER TABLE %s ALTER COLUMN code TYPE bytea USING convert_to(code, 'UTF8')`, dstFull))
	e2e.EnvNoError(t, env, err)

	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES ('\\x41', 2)`, srcFull)))
	catalog, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	e2e.EnvNoError(t, env, err)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "normalize refusing text into bytea", func() bool {
		n, err := e2e.GetLogCount(t.Context(), catalog, e2e.AddSuffix(s, srcTable), "error",
			`column "code" is of type bytea but expression is of type text`)
		return err == nil && n > 0
	})
	require.Equal(t, int64(1), s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)), "row stored")
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

// startTargetTypesFlow starts a mirror of the given tables, each into its own destination table
func (s MySQLPGSuite) startTargetTypesFlow(srcTables ...string) e2e.WorkflowRun {
	pairs := make([]string, 0, 2*len(srcTables))
	for _, table := range srcTables {
		pairs = append(pairs, table, table)
	}
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTables[0]),
		TableMappings: e2e.TableMappings(s, pairs...),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t, tc, flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	return env
}
