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
	// the migration guide's target prerequisite for spatial columns
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
// for the old key, so the MySQL key-update failure is expected not to reproduce here.
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
// check runs against both rows and all mismatches are reported together.
func (s CRDBPGSuite) Test_Type_Fidelity_Snapshot_And_CDC() {
	t := s.t
	srcTable := "type_fidelity"
	schema := Schema(s)
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TYPE %s.mood AS ENUM ('happy', 'sad')`, schema)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id INT8 PRIMARY KEY,
		c_oid OID, c_dec_nan DECIMAL, c_dec_inf DECIMAL, c_dec_big DECIMAL,
		c_interval INTERVAL, c_timetz TIMETZ, c_time24 TIME,
		c_jsonb JSONB, c_jsonb_null JSONB,
		c_int_arr INT8[], c_text_arr STRING[],
		c_date_bc DATE, c_ts_bc TIMESTAMP,
		c_geom GEOMETRY, c_bytes_empty BYTES, c_str_empty STRING,
		c_mood %s.mood, c_bit BIT(4), c_inet INET, c_uuid UUID
	)`, srcFull, schema)))
	values := `(%d,
		3000000000, 'NaN', 'Infinity', 12345678901234567890.123456789012345678,
		'1 year 2 mons 3 days 04:05:06.789', '13:14:15+05:30', '24:00:00',
		'{"b": 1, "a": [1, null]}', 'null',
		ARRAY[1, NULL, 3], ARRAY['x', NULL, ''],
		'0044-03-15 BC', '0044-03-15 12:00:00 BC',
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
		{"c_oid", `c_oid::text = '3000000000'`, `c_oid::text`},
		{"c_dec_nan", `c_dec_nan::text = 'NaN'`, `c_dec_nan::text`},
		{"c_dec_inf", `c_dec_inf::text = 'Infinity'`, `c_dec_inf::text`},
		{"c_dec_big", `c_dec_big::text = '12345678901234567890.123456789012345678'`, `c_dec_big::text`},
		{"c_interval", `c_interval::text::interval = '1 year 2 mons 3 days 04:05:06.789'::interval`, `c_interval::text`},
		{"c_timetz", `c_timetz::text = '13:14:15+05:30'`, `c_timetz::text`},
		{"c_time24", `c_time24::text = '24:00:00'`, `c_time24::text`},
		{"c_jsonb", `c_jsonb::jsonb = '{"a": [1, null], "b": 1}'::jsonb`, `c_jsonb::text`},
		{"c_jsonb_null", `c_jsonb_null IS NOT NULL AND c_jsonb_null::jsonb = 'null'::jsonb`, `coalesce(c_jsonb_null::text, 'SQL NULL')`},
		{"c_int_arr", `c_int_arr::text = '{1,NULL,3}'`, `c_int_arr::text`},
		{"c_text_arr", `c_text_arr::text = '{x,NULL,""}'`, `c_text_arr::text`},
		{"c_date_bc", `c_date_bc::text = '0044-03-15 BC'`, `c_date_bc::text`},
		{"c_ts_bc", `c_ts_bc::text = '0044-03-15 12:00:00 BC'`, `c_ts_bc::text`},
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
// MySQL (cascades aren't in the binlog) the children should disappear on the destination.
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
