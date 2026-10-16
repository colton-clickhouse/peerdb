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
	"github.com/PeerDB-io/peerdb/flow/internal"
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
	// spatial columns need PostGIS on the target
	_, err = dest.Conn().Exec(t.Context(), "CREATE EXTENSION IF NOT EXISTS postgis")
	require.NoError(t, err, "failed to create postgis on the destination")

	return CRDBPGSuite{t: t, source: source, dest: dest, suffix: suffix}
}

// The generic tests (Test_Simple_Flow, ...) with a CockroachDB source and a Postgres destination.
func TestGenericCRDBPG(t *testing.T) {
	setup := SetupGenericSuite(setupGenericCRDBPGSuite)
	e2eshared.RunSuite(t, func(t *testing.T) genericCRDBPG { return genericCRDBPG{setup(t)} })
}

// genericCRDBPG runs the generic tests, except two that expect an integer column's typmod to be 0 for any source but
// Postgres and MySQL. CockroachDB reports -1 for it, as Postgres does, which the shared test doesn't allow for; the
// schema these tests check is otherwise the same.
type genericCRDBPG struct {
	Generic
}

func (s genericCRDBPG) Test_Schema_Change_Lost_Column_Bug() {
	s.T().Skip("expects typmod 0 for an integer column from a source other than Postgres or MySQL; CockroachDB reports" +
		" -1, as Postgres does")
}

func (s genericCRDBPG) Test_Schema_Change_Drop_Consecutive_Columns() {
	s.T().Skip("expects typmod 0 for an integer column from a source other than Postgres or MySQL; CockroachDB reports" +
		" -1, as Postgres does")
}

// setupGenericCRDBPGSuite is SetupCRDBPGSuite for the generic tests, which are written for a Postgres source: they
// expect SERIAL and INTEGER columns to be 32-bit. CockroachDB makes SERIAL a unique_rowid() INT8 and INTEGER an INT8
// unless serial_normalization is sql_sequence and default_int_size is 4. The source's DDL runs on one connection, so
// the session settings hold for the whole test.
func setupGenericCRDBPGSuite(t *testing.T) CRDBPGSuite {
	t.Helper()
	s := SetupCRDBPGSuite(t)
	for _, stmt := range []string{"SET default_int_size = 4", "SET serial_normalization = 'sql_sequence'"} {
		require.NoError(t, s.source.Exec(t.Context(), stmt))
	}
	return s
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
	// the snapshot keeps NULL (the table-equality helper above treats NULL and '' as equal)
	EnvTrue(t, env, s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 AND note IS NULL`, dstFull)) == 1)

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (3, 'three', 'cdc', 3.25)`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET note = 'updated' WHERE id = 1`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE id = 2`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc insert/update/delete", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id = 1 AND note = 'updated') OR (id = 3 AND amount = 3.25)`, dstFull)) == 2 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	// every column, and NULL told apart from '' (the table-equality helper above treats them as equal)
	require.Equal(t, "1|one|updated|1.50,3|three|cdc|3.25", s.destText(fmt.Sprintf(
		`SELECT string_agg(concat_ws('|', id, name, coalesce(note, '<NULL>'), amount), ',' ORDER BY id) FROM %s`, dstFull)))
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
	// the rows that remain are the ones the source kept
	require.Equal(t, "20:2", s.destText(fmt.Sprintf(`SELECT string_agg(id || ':' || parent_id, ',') FROM %s`,
		s.DestinationTable(child))))
	require.Equal(t, "2", s.destText(fmt.Sprintf(`SELECT string_agg(id::text, ',') FROM %s`, s.DestinationTable(parent))))

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// A NULL array must stay NULL and an empty array empty, on both paths (via CDC a NULL array
// arrived as '{}'). NULL elements are tested separately.
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

// OID columns through CDC (the changefeed JSON carries OIDs as quoted strings, the decoder wanted a
// bare number, and the whole mirror stopped). Values stay below 2^31 (above that needs OIDs mapped to bigint) and
// no array is NULL (another test covers that).
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

// OIDs above 2^31 - 1 through snapshot and CDC: needs both the bigint mapping
// (InternalVersion_PgWidenUnsignedInts) and the quoted-OID decoding.
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

// TIMETZ through snapshot and CDC (the snapshot's binary COPY sent an 8-byte time for the 12-byte
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

// NUL bytes in strings (PG text can't hold U+0000, so the snapshot's COPY failed with 22021 and retried
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

// A clear source failure must show in the mirror's status (a table with several column families made every
// changefeed start fail while the mirror reported STATUS_RUNNING). The changefeed error is user-actionable
// (NOTIFY_CHANGEFEED_INVALID), so once it has repeated without progress for PEERDB_STALLED_THRESHOLD_MINUTES the
// mirror is STATUS_STALLED, and its log says why.
func (s CRDBPGSuite) Test_Column_Families_Stalls_Mirror() {
	t := s.t
	srcTable := "families_stall"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT8 PRIMARY KEY, hot INT8, cold STRING, FAMILY f_hot (id, hot), FAMILY f_cold (cold))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 10, 'a')`, srcFull)))

	client, err := NewApiClient()
	require.NoError(t, err)
	SetDynamicSetting(t, "PEERDB_STALLED_THRESHOLD_MINUTES", "1")

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 2*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvWaitFor(t, env, 6*time.Minute, "STALLED while every changefeed start fails", func() bool {
		res, err := client.MirrorStatus(t.Context(), &protos.MirrorStatusRequest{FlowJobName: flowConnConfig.FlowJobName})
		if err != nil {
			t.Log("mirror status:", err)
			return false
		}
		return res.CurrentFlowState == protos.FlowStatus_STATUS_STALLED
	})
	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "warn",
		"Mirror is STALLED%NOTIFY_CHANGEFEED_INVALID")
	EnvNoError(t, env, err)
	require.Positive(t, count, "the mirror's log says why it is STALLED")
	count, err = GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "error",
		"recreate the table with a single column family")
	EnvNoError(t, env, err)
	require.Positive(t, count, "the mirror's errors say how to fix it")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
