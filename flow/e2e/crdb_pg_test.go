package e2e

// CockroachDB source -> Postgres destination. v0.37.10 has no such suite: the CockroachDB e2e suites
// all target ClickHouse. Added for the CockroachDB -> Managed Postgres work; the source comes from
// CI_COCKROACH_* like the other CockroachDB suites.
//
// Tests assert what a correct migration looks like, so a failure is a finding (or a repro of one).
// Values are checked with direct SQL on the destination (destBool / destInt): the table-equality
// helpers treat NULL / empty values as equal and decode the source with the snapshot's own code.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type CRDBPGSuite struct {
	t      *testing.T
	source *CockroachDBSource
	dest   *PostgresSource
	suffix string
}

func (s CRDBPGSuite) T() *testing.T {
	return s.t
}

func (s CRDBPGSuite) Suffix() string {
	return s.suffix
}

func (s CRDBPGSuite) Source() SuiteSource {
	return s.source
}

func (s CRDBPGSuite) Connector() *connpostgres.PostgresConnector {
	return s.dest.PostgresConnector
}

func (s CRDBPGSuite) DestinationConnector() connectors.Connector {
	return s.dest.PostgresConnector
}

func (s CRDBPGSuite) Peer() *protos.Peer {
	return GeneratePostgresPeer(s.t)
}

func (s CRDBPGSuite) DestinationTable(table string) string {
	return AttachSchema(s, table)
}

func (s CRDBPGSuite) GetRows(table string, cols string) (*model.QRecordBatch, error) {
	s.t.Helper()
	pgQueryExecutor, err := s.dest.NewQRepQueryExecutor(s.t.Context(), nil, shared.InternalVersion_Latest, "testflow", "testpart")
	if err != nil {
		return nil, err
	}

	return pgQueryExecutor.ExecuteAndProcessQuery(
		s.t.Context(),
		fmt.Sprintf(`SELECT %s FROM e2e_test_%s.%s ORDER BY id`, cols, s.suffix, common.QuoteIdentifier(table)),
	)
}

func (s CRDBPGSuite) Teardown(ctx context.Context) {
	s.source.Teardown(s.t, ctx, s.suffix)
	TearDownPostgres(ctx, s)
}

func SetupCRDBPGSuite(t *testing.T) CRDBPGSuite {
	t.Helper()

	suffix := "crdbpg_" + strings.ToLower(common.RandomString(8))
	source, err := SetupCockroachDB(t, suffix)
	require.NoError(t, err, "failed to setup cockroachdb")
	dest, err := SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")
	// the migration guide's target prerequisite for spatial columns (MySQL -> PG finding #2)
	_, err = dest.Conn().Exec(t.Context(), "CREATE EXTENSION IF NOT EXISTS postgis")
	require.NoError(t, err, "failed to create postgis on the destination")

	return CRDBPGSuite{t: t, source: source, dest: dest, suffix: suffix}
}

// The generic tests (Test_Simple_Flow, ...) with a CockroachDB source and a Postgres destination.
func TestGenericCRDBPG(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupCRDBPGSuite))
}

func TestCRDBPG(t *testing.T) {
	e2eshared.RunSuite(t, SetupCRDBPGSuite)
}

func (s CRDBPGSuite) destInt(query string) int64 {
	s.t.Helper()
	var n int64
	require.NoError(s.t, s.dest.Conn().QueryRow(s.t.Context(), query).Scan(&n), query)
	return n
}

// destBool runs a boolean query on the destination; a query error counts as false and is returned.
func (s CRDBPGSuite) destBool(query string) (bool, error) {
	var b *bool
	if err := s.dest.Conn().QueryRow(s.t.Context(), query).Scan(&b); err != nil {
		return false, err
	}
	return b != nil && *b, nil
}

// destText returns a value rendered as text on the destination, for failure messages.
func (s CRDBPGSuite) destText(query string) string {
	var v *string
	if err := s.dest.Conn().QueryRow(s.t.Context(), query).Scan(&v); err != nil {
		return "error: " + err.Error()
	}
	if v == nil {
		return "NULL"
	}
	return *v
}

func (s CRDBPGSuite) startMirror(srcTable string) *protos.FlowConnectionConfigs {
	s.t.Helper()
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	return flowConnConfig
}

// Canary: snapshot plus CDC insert / update / delete with plain types. It should pass on every build;
// if it fails, suspect the environment before the code under test.
func (s CRDBPGSuite) Test_Canary_Snapshot_And_CDC() {
	t := s.t
	srcTable := "canary"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, name STRING, note STRING, amount DECIMAL(10,2))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES (1, 'one', 'snapshot', 1.50), (2, 'two', NULL, 2.00)`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, srcTable, "id,name,note,amount")

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (3, 'three', 'cdc', 3.25)`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET note = 'updated' WHERE id = 1`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE id = 2`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc insert/update/delete", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id = 1 AND note = 'updated') OR (id = 3 AND amount = 3.25)`, dstFull)) == 2 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// A primary-key UPDATE must remove the row under its old key. CockroachDB's changefeed emits a delete
// for the old key, so MySQL -> PG finding #19 is expected not to reproduce here.
func (s CRDBPGSuite) Test_PKey_Update_Removes_Old_Row() {
	t := s.t
	srcTable := "pk_probe"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, v STRING)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, srcTable, "id,v")

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET id = 101 WHERE id = 1`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET id = 102, v = 'b2' WHERE id = 2`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 'c2' WHERE id = 3`, srcFull)))

	EnvWaitFor(t, env, 3*time.Minute, "updated rows under their new keys", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id, v) IN ((101, 'a'), (102, 'b2'), (3, 'c2'))`, dstFull)) == 3
	})
	oldKeys := s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id IN (1, 2)`, dstFull))
	total := s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull))

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.EqualValues(t, 0, oldKeys, "rows under the old primary keys were left behind")
	require.EqualValues(t, 3, total, "destination row count")
}

// A table without a primary key replicates through CockroachDB's hidden rowid (schema.go treats such
// tables as always mirrorable). Nothing upstream tests it, and it is unclear whether the changefeed
// payload carries rowid. Checks snapshot and CDC insert / update / delete, including exact duplicates.
func (s CRDBPGSuite) Test_NoPK_Table_Snapshot_And_CDC() {
	t := s.t
	srcTable := "no_pk"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (k INT8, v STRING)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot: 4 rows incl. the duplicate", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(
			`SELECT count(*) = 4 AND count(*) FILTER (WHERE k = 1 AND v = 'a') = 2 FROM %s`, dstFull))
		return ok
	})

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (4, 'd'), (4, 'd')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 'b2' WHERE k = 2`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE k = 3`, srcFull)))

	var got string
	EnvWaitFor(t, env, 3*time.Minute, "cdc on the keyless table", func() bool {
		got = s.destText(fmt.Sprintf(`SELECT string_agg(k || ':' || v, ',' ORDER BY k, v) FROM %s`, dstFull))
		return got == "1:a,1:a,2:b2,4:d,4:d"
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Value fidelity for types the connector maps lossily or decodes differently in snapshot (pgx binary)
// and CDC (changefeed JSON). Row 1 arrives by snapshot and row 2 (identical values) by CDC; every
// check runs against both rows and all mismatches are reported together. TIMETZ, NUL in STRING and
// OID above 2^31-1 block the snapshot (Test_Types_That_Block_Snapshot, Test_OID_Above_Int32_Max) and BC
// dates block CDC normalization (Test_BC_Dates_Via_CDC), so they are not in this table.
func (s CRDBPGSuite) Test_Type_Fidelity_Snapshot_And_CDC() {
	t := s.t
	srcTable := "type_fidelity"
	schema := Schema(s)
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TYPE %s.mood AS ENUM ('happy', 'sad')`, schema)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id INT8 PRIMARY KEY,
		c_dec_nan DECIMAL, c_dec_inf DECIMAL, c_dec_big DECIMAL,
		c_interval INTERVAL, c_time24 TIME,
		c_jsonb JSONB, c_jsonb_null JSONB,
		c_int_arr INT8[], c_text_arr STRING[],
		c_geom GEOMETRY, c_bytes_empty BYTES, c_str_empty STRING,
		c_mood %s.mood, c_bit BIT(4), c_inet INET, c_uuid UUID
	)`, srcFull, schema)))
	values := `(%d,
		'NaN', 'Infinity', 12345678901234567890.123456789012345678,
		'1 year 2 mons 3 days 04:05:06.789', '24:00:00',
		'{"b": 1, "a": [1, null]}', 'null',
		ARRAY[1, NULL, 3], ARRAY['x', NULL, ''],
		'SRID=3857;POINT(1 2)', ''::BYTES, '',
		'sad', B'1010', '192.168.1.0/24', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')`
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES `+values, srcFull, 1)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 1`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES `+values, srcFull, 2)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2`, dstFull)) == 1
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	// column -> (predicate that a faithful copy satisfies, expression to show when it doesn't)
	checks := []struct{ col, ok, show string }{
		{"c_dec_nan", `c_dec_nan::text = 'NaN'`, `c_dec_nan::text`},
		{"c_dec_inf", `c_dec_inf::text = 'Infinity'`, `c_dec_inf::text`},
		{"c_dec_big", `c_dec_big::text = '12345678901234567890.123456789012345678'`, `c_dec_big::text`},
		{"c_interval", `c_interval::text::interval = '1 year 2 mons 3 days 04:05:06.789'::interval`, `c_interval::text`},
		{"c_time24", `c_time24::text = '24:00:00'`, `c_time24::text`},
		{"c_jsonb", `c_jsonb::jsonb = '{"a": [1, null], "b": 1}'::jsonb`, `c_jsonb::text`},
		{"c_jsonb_null", `c_jsonb_null IS NOT NULL AND c_jsonb_null::jsonb = 'null'::jsonb`, `coalesce(c_jsonb_null::text, 'SQL NULL')`},
		{"c_int_arr", `c_int_arr::text = '{1,NULL,3}'`, `c_int_arr::text`},
		{"c_text_arr", `c_text_arr::text = '{x,NULL,""}'`, `c_text_arr::text`},
		{"c_geom", `ST_SRID(c_geom) = 3857 AND ST_AsText(c_geom) = 'POINT(1 2)'`, `ST_AsEWKT(c_geom)`},
		{"c_bytes_empty", `c_bytes_empty IS NOT NULL AND length(c_bytes_empty) = 0`, `coalesce(encode(c_bytes_empty, 'hex'), 'NULL')`},
		{"c_str_empty", `c_str_empty IS NOT NULL AND c_str_empty = ''`, `coalesce(c_str_empty, 'NULL')`},
		{"c_mood", `c_mood::text = 'sad'`, `c_mood::text`},
		{"c_bit", `c_bit::text = '1010'`, `c_bit::text`},
		{"c_inet", `c_inet::text = '192.168.1.0/24'`, `c_inet::text`},
		{"c_uuid", `c_uuid::text = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`, `c_uuid::text`},
	}
	types := map[string]string{}
	rows, err := s.dest.Conn().Query(t.Context(), `SELECT column_name, format_type(atttypid, atttypmod)
		FROM information_schema.columns c JOIN pg_attribute a ON a.attname = c.column_name
		AND a.attrelid = (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass
		WHERE table_schema = $1 AND table_name = $2`, "e2e_test_"+s.suffix, srcTable)
	require.NoError(t, err)
	for rows.Next() {
		var c, ty string
		require.NoError(t, rows.Scan(&c, &ty))
		types[c] = ty
	}
	require.NoError(t, rows.Err())

	var bad []string
	for _, c := range checks {
		for id, via := range map[int]string{1: "snapshot", 2: "cdc"} {
			ok, err := s.destBool(fmt.Sprintf(`SELECT %s FROM %s WHERE id = %d`, c.ok, dstFull, id))
			if !ok {
				got := s.destText(fmt.Sprintf(`SELECT %s FROM %s WHERE id = %d`, c.show, dstFull, id))
				if err != nil {
					got += " (check failed: " + err.Error() + ")"
				}
				bad = append(bad, fmt.Sprintf("%-14s %-8s dest type %-28s got %s", c.col, via, types[c.col], got))
			}
		}
	}
	if len(bad) > 0 {
		t.Errorf("%d value mismatches on the destination:\n  %s", len(bad), strings.Join(bad, "\n  "))
	}
}

// Which types stop the initial snapshot? One single-column table per type, all in one mirror; a table
// whose row never reaches the destination is retried forever and keeps the mirror in STATUS_SNAPSHOT.
// The failure message lists the blocking types (diagnostic for Test_Type_Fidelity_Snapshot_And_CDC).
func (s CRDBPGSuite) Test_Types_That_Block_Snapshot() {
	t := s.t
	schema := Schema(s)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TYPE %s.mood2 AS ENUM ('happy', 'sad')`, schema)))
	cases := []struct{ name, typ, val string }{
		{"dec_nan", "DECIMAL", "'NaN'"}, {"dec_inf", "DECIMAL", "'Infinity'"},
		{"dec_big", "DECIMAL", "12345678901234567890.123456789012345678"},
		{"interval", "INTERVAL", "'1 year 2 mons 3 days 04:05:06.789'"}, {"timetz", "TIMETZ", "'13:14:15+05:30'"},
		{"time24", "TIME", "'24:00:00'"}, {"jsonb", "JSONB", `'{"b": 1, "a": [1, null]}'`}, {"jsonb_null", "JSONB", "'null'"},
		{"int_arr", "INT8[]", "ARRAY[1, NULL, 3]"}, {"text_arr", "STRING[]", "ARRAY['x', NULL, '']"},
		{"date_bc", "DATE", "'0044-03-15 BC'"}, {"ts_bc", "TIMESTAMP", "'0044-03-15 12:00:00 BC'"},
		{"date_inf", "DATE", "'infinity'"}, {"ts_inf", "TIMESTAMPTZ", "'infinity'"},
		{"geom", "GEOMETRY", "'SRID=3857;POINT(1 2)'"}, {"bytes_empty", "BYTES", "''::BYTES"},
		{"mood", schema + ".mood2", "'sad'"}, {"bit", "BIT(4)", "B'1010'"}, {"inet", "INET", "'192.168.1.0/24'"},
		{"interval_arr", "INTERVAL[]", "ARRAY['1 day'::INTERVAL]"}, {"str_nul", "STRING", `e'a\x00b'`},
	}
	tables := make([]string, 0, 2*len(cases))
	for _, c := range cases {
		tbl := "blk_" + c.name
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, v %s)`, AttachSchema(s, tbl), c.typ)))
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, %s)`, AttachSchema(s, tbl), c.val)))
		tables = append(tables, tbl, tbl)
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, "blk"),
		TableMappings: TableMappings(s, tables...),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 8
	flowConnConfig.SnapshotNumTablesInParallel = 8
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)

	arrived := func(tbl string) bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT count(*) = 1 FROM %s`, s.DestinationTable(tbl)))
		return ok
	}
	deadline := time.Now().Add(2 * time.Minute)
	var blocked []string
	for time.Now().Before(deadline) {
		blocked = blocked[:0]
		for _, c := range cases {
			if !arrived("blk_" + c.name) {
				blocked = append(blocked, c.name+" ("+c.typ+")")
			}
		}
		if len(blocked) == 0 {
			break
		}
		time.Sleep(5 * time.Second)
	}
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	require.Empty(t, blocked, "types whose snapshot never reached the destination")
}

// BC dates and timestamps: the snapshot copies them (binary), but CDC renders them from changefeed JSON
// with Go's astronomical year numbering ("-0043-03-15" for 44 BC), which Postgres rejects in the
// normalize cast. Expected failure: normalization errors on every batch and stalls the whole mirror.
func (s CRDBPGSuite) Test_BC_Dates_Via_CDC() {
	t := s.t
	srcTable := "bc_dates"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, d DATE, ts TIMESTAMP)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, '0044-03-15 BC', '0044-03-15 12:00:00 BC')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 2*time.Minute, "snapshot row", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT d::text = '0044-03-15 BC' AND ts::text = '0044-03-15 12:00:00 BC' FROM %s WHERE id = 1`, dstFull))
		return ok
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, '0044-03-15 BC', '0044-03-15 12:00:00 BC'), (3, '2026-01-01', '2026-01-01 00:00:00')`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "cdc rows with the same values", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT count(*) = 2 FROM %s WHERE id IN (2, 3) AND (id = 3 OR (d::text = '0044-03-15 BC' AND ts::text = '0044-03-15 12:00:00 BC'))`, dstFull))
		return ok
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// A hash-sharded primary key (CockroachDB's recommended layout for sequential keys) puts a hidden,
// virtual shard column in the key. The connector leaves virtual columns out of the schema but keeps
// the key, so the destination CREATE TABLE names a missing column (42703) and setup retries forever
// for every table in the mirror. Expected failure: the table never reaches the destination.
func (s CRDBPGSuite) Test_Hash_Sharded_PKey() {
	t := s.t
	srcTable := "hash_sharded"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (ts TIMESTAMPTZ NOT NULL, id INT8 NOT NULL, v STRING, PRIMARY KEY (ts, id) USING HASH)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES ('2026-01-01', 1, 'a'), ('2026-01-02', 2, 'b')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 2*time.Minute, "snapshot rows", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT count(*) = 2 FROM %s`, dstFull))
		return ok
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 'b2' WHERE id = 2`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "cdc update", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT count(*) = 1 FROM %s WHERE id = 2 AND v = 'b2'`, dstFull))
		return ok
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// A table with more than one column family: CockroachDB refuses a changefeed on it unless
// split_column_families is set, which the connector doesn't do. Expected failure: the snapshot
// completes but the changefeed fails after 5 retries, so CDC stops for every table in the mirror.
func (s CRDBPGSuite) Test_Column_Families_CDC() {
	t := s.t
	srcTable := "families"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT8 PRIMARY KEY, hot INT8, cold STRING, FAMILY f_hot (id, hot), FAMILY f_cold (cold))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 10, 'a')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 2*time.Minute, "snapshot row", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT count(*) = 1 FROM %s`, dstFull))
		return ok
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET hot = 11 WHERE id = 1`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 20, 'b')`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "cdc update and insert, whole rows", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(
			`SELECT string_agg(id || ':' || hot || ':' || cold, ',' ORDER BY id) = '1:11:a,2:20:b' FROM %s`, dstFull))
		return ok
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// OID is an unsigned 32-bit integer; the connector maps it to UInt32 and the Postgres destination to
// integer (int4), so values above 2^31-1 can't be written. Expected failure: the snapshot partition is
// retried forever and the mirror never leaves STATUS_SNAPSHOT.
func (s CRDBPGSuite) Test_OID_Above_Int32_Max() {
	t := s.t
	srcTable := "oid_big"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, o OID)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES (1, 1), (2, 2147483647), (3, 3000000000), (4, 4294967295)`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 2*time.Minute, "all 4 OID values on the destination", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT string_agg(o::text, ',' ORDER BY id) = '1,2147483647,3000000000,4294967295' FROM %s`, dstFull))
		return ok
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// ADD COLUMN ... DEFAULT during CDC. The changefeed runs with schema_change_policy='nobackfill', so
// the expected failure is that pre-existing destination rows keep NULL instead of the default.
func (s CRDBPGSuite) Test_Add_Column_Default_Backfills_Existing_Rows() {
	t := s.t
	srcTable := "add_col"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, v STRING)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a'), (2, 'b')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, srcTable, "id,v")

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s ADD COLUMN status STRING NOT NULL DEFAULT 'new'`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id, v) VALUES (3, 'c')`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "new column and row 3 on the destination", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT count(*) = 1 FROM %s WHERE id = 3 AND status = 'new'`, dstFull))
		return ok
	})
	got := s.destText(fmt.Sprintf(`SELECT string_agg(id || ':' || coalesce(status, 'NULL'), ',' ORDER BY id) FROM %s`, dstFull))

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, "1:new,2:new,3:new", got, "pre-existing rows should carry the new column's default")
}

// ON DELETE CASCADE: CockroachDB writes the cascaded child deletes as ordinary row changes, so unlike
// MySQL (finding #21, cascades aren't in the binlog) the children should disappear on the destination.
func (s CRDBPGSuite) Test_FK_Cascade_Delete_Replicates() {
	t := s.t
	parent, child := "fk_parent", "fk_child"
	pFull, cFull := AttachSchema(s, parent), AttachSchema(s, child)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY)`, pFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT8 PRIMARY KEY, parent_id INT8 REFERENCES %s (id) ON DELETE CASCADE)`, cFull, pFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1), (2)`, pFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (10, 1), (11, 1), (20, 2)`, cFull)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, parent),
		TableMappings: TableMappings(s, parent, parent, child, child),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "initial load", child, child, "id,parent_id")

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE id = 1`, pFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cascaded child deletes", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(child))) == 1 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(parent))) == 1
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// A NULL array must stay NULL and an empty array empty, on both paths (#28: via CDC a NULL array
// arrived as '{}'). Element NULLs are #10, not tested here.
func (s CRDBPGSuite) Test_Null_Array_Stays_Null() {
	if bad := s.nullArrayProblems("null_array", shared.InternalVersion_Latest); len(bad) > 0 {
		s.t.Errorf("%d array mismatches on the destination:\n  %s", len(bad), strings.Join(bad, "\n  "))
	}
}

// Mirrors created before InternalVersion_PgNullArrayStaysNull keep writing '{}' for a NULL array through CDC, so a
// running mirror doesn't switch mid-stream. The snapshot keeps NULL on every version.
func (s CRDBPGSuite) Test_Null_Array_Old_Version_Keeps_Empty() {
	bad := s.nullArrayProblems("null_array_old", shared.InternalVersion_PgNullArrayStaysNull-1)
	want := []string{"id 3: want NULL NULL, got {} {}", "id 11: want NULL NULL, got {} {}"}
	require.ElementsMatch(s.t, want, bad)
}

// nullArrayProblems replicates NULL, empty and non-empty arrays through snapshot and CDC on a mirror of the given
// version and returns the rows that differ from the source.
func (s CRDBPGSuite) nullArrayProblems(srcTable string, version uint32) []string {
	t := s.t
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, ia INT8[], sa STRING[])`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES
		(1, NULL, NULL), (2, '{}', '{}'), (3, '{1,2}', '{a,b}')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	// mirror creation replaces Version with PEERDB_FORCE_INTERNAL_VERSION (default: latest)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(version), 10)}
	flowConnConfig.Version = version
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 3
	})

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES
		(11, NULL, NULL), (12, '{}', '{}'), (13, '{1,2}', '{a,b}')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET ia = NULL, sa = NULL WHERE id = 3`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (99, '{9}', '{z}')`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 99`, dstFull)) == 1
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	want := map[int]string{1: "NULL NULL", 2: "{} {}", 3: "NULL NULL", 11: "NULL NULL", 12: "{} {}", 13: "{1,2} {a,b}"}
	var bad []string
	for _, id := range []int{1, 2, 3, 11, 12, 13} {
		got := s.destText(fmt.Sprintf(`SELECT coalesce(ia::text, 'NULL') || ' ' || coalesce(sa::text, 'NULL') FROM %s WHERE id = %d`,
			dstFull, id))
		if got != want[id] {
			bad = append(bad, fmt.Sprintf("id %d: want %s, got %s", id, want[id], got))
		}
	}
	return bad
}

// OID columns through CDC (#27: the changefeed JSON carries OIDs as quoted strings, the decoder wanted a
// bare number, and the whole mirror stopped). Values stay below 2^31 (above is #1, OID -> integer) and
// no array is NULL (that is #28).
func (s CRDBPGSuite) Test_OID_Via_CDC() {
	t := s.t
	srcTable := "oid_cdc"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, o OID, oa OID[])`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1, ARRAY[1, 2147483647]::OID[])`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})

	EnvNoError(t, env, s.source.Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES (2, 2147483647, ARRAY[3, 4]::OID[]), (3, NULL, '{}'::OID[])`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET o = 5 WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc insert/update of OID values", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT string_agg(id || ':' || coalesce(o::text, 'NULL') || ':' || coalesce(oa::text, 'NULL'),
			' ' ORDER BY id) = '1:5:{1,2147483647} 2:2147483647:{3,4} 3:NULL:{}' FROM %s`, dstFull))
		return ok
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// OIDs above 2^31 - 1 through snapshot and CDC: needs both the bigint mapping (#1, from MySQL -> PG #5/#20's
// InternalVersion_PgWidenUnsignedInts) and the quoted-OID decoding (#27).
func (s CRDBPGSuite) Test_OID_Above_Int32_Max_Snapshot_And_CDC() {
	t := s.t
	srcTable := "oid_big_cdc"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, o OID)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 1), (2, 4294967295)`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (3, 3000000000)`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET o = 2147483648 WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT string_agg(id || ':' || o::text, ' ' ORDER BY id)
			= '1:2147483648 2:4294967295 3:3000000000' FROM %s`, dstFull))
		return ok
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// TIMETZ through snapshot and CDC (#2: the snapshot's binary COPY sent an 8-byte time for the 12-byte
// timetz, PG refused it with 08P01 and the snapshot retried forever). PeerDB keeps the UTC instant, not the
// source offset, so compare in UTC; '23:30-05' and '01:00+05' cross midnight in UTC.
func (s CRDBPGSuite) Test_TimeTZ_Snapshot_And_CDC() {
	t := s.t
	srcTable := "timetz"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	values := `(%[1]d, '10:00:00+02'), (%[1]d + 1, '23:30:00-05'), (%[1]d + 2, '01:00:00+05'), (%[1]d + 3, '12:34:56.789+00')`
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, c TIMETZ)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES `+fmt.Sprintf(values, 1), srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 4
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES `+fmt.Sprintf(values, 11), srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 8
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	want := []string{"08:00:00+00", "04:30:00+00", "20:00:00+00", "12:34:56.789+00"}
	var bad []string
	for base, via := range map[int]string{1: "snapshot", 11: "cdc"} {
		for i, w := range want {
			got := s.destText(fmt.Sprintf(`SELECT (c AT TIME ZONE 'UTC')::text FROM %s WHERE id = %d`, dstFull, base+i))
			if got != w {
				bad = append(bad, fmt.Sprintf("id %d (%s): want %s, got %s", base+i, via, w, got))
			}
		}
	}
	if len(bad) > 0 {
		t.Errorf("%d timetz mismatches on the destination:\n  %s", len(bad), strings.Join(bad, "\n  "))
	}
}

// NUL bytes in strings (#3: PG text can't hold U+0000, so the snapshot's COPY failed with 22021 and retried
// forever; via CDC the raw table's JSONB refused \u0000). PeerDB strips them on the way into PG: a
// deliberate behavioral difference, the value is not identical to the source.
func (s CRDBPGSuite) Test_Nul_Bytes_Stripped() {
	t := s.t
	srcTable := "nul_strip"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, s STRING, sa STRING[])`, srcFull)))
	values := `(%[1]d, e'a\x00b', ARRAY[e'\x00x', 'y']), (%[1]d + 1, 'plain', ARRAY['p'])`
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES `+fmt.Sprintf(values, 1), srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES `+fmt.Sprintf(values, 11), srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET s = e'\x00\x00c' WHERE id = 2`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT string_agg(id || ':' || s || ':' || sa::text, ' ' ORDER BY id)
			= '1:ab:{x,y} 2:c:{p} 11:ab:{x,y} 12:plain:{p}' FROM %s`, dstFull))
		return ok
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// A clear source failure must fail the mirror visibly (#18: a table with several column families made every
// changefeed start fail while the mirror reported STATUS_RUNNING). Expected: STATUS_FAILED once CDC starts; resuming
// retries (and fails again, the table still has two families), so the mirror ends up FAILED, not RUNNING.
func (s CRDBPGSuite) Test_Column_Families_Fails_Mirror() {
	t := s.t
	srcTable := "families_fail"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT8 PRIMARY KEY, hot INT8, cold STRING, FAMILY f_hot (id, hot), FAMILY f_cold (cold))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 10, 'a')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	// not SetupCDCFlowStatusQuery: it requires STATUS_RUNNING, and the mirror may already have failed
	EnvWaitFor(t, env, 2*time.Minute, "snapshot row", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT count(*) = 1 FROM %s`, dstFull)) // the table may not exist yet
		return ok
	})
	EnvWaitFor(t, env, 4*time.Minute, "mirror failed", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_FAILED
	})

	// FAILED is set just before the workflow continues as new into its paused wait; a signal sent inside that window
	// goes to the old run and is dropped, so let it settle (a resume sent that early can simply be repeated)
	time.Sleep(5 * time.Second)
	failedRun := EnvGetRunID(t, env)

	// resume with new settings, as for a frozen cursor (#31): the update must reach the failed mirror too. The retry
	// fails again within about a second (the table still has two families), so check the outcome, not RUNNING
	client, err := NewApiClient()
	require.NoError(t, err)
	_, err = client.FlowStateChange(t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
			CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{IdleTimeout: 77},
		}},
	})
	require.NoError(t, err)
	EnvWaitFor(t, env, 4*time.Minute, "retried with the new sync interval, failed again", func() bool {
		return EnvGetRunID(t, env) != failedRun &&
			EnvGetWorkflowState(t, env).SyncFlowOptions.IdleTimeoutSeconds == 77 &&
			env.GetFlowStatus(t) == protos.FlowStatus_STATUS_FAILED
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
