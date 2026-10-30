package e2e

// MySQL source -> Postgres destination. v0.37.10 has no such suite: PeerFlowE2ETestSuitePG is
// Postgres -> Postgres and every MySQL suite targets ClickHouse. Added for the MySQL -> Managed
// Postgres work; the MySQL flavor comes from CI_MYSQL_* like the other MySQL suites.

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

type MySQLPGSuite struct {
	t      *testing.T
	source *MySqlSource
	dest   *PostgresSource
	suffix string
}

func (s MySQLPGSuite) T() *testing.T {
	return s.t
}

func (s MySQLPGSuite) Suffix() string {
	return s.suffix
}

func (s MySQLPGSuite) Source() SuiteSource {
	return s.source
}

func (s MySQLPGSuite) Connector() *connpostgres.PostgresConnector {
	return s.dest.PostgresConnector
}

func (s MySQLPGSuite) DestinationConnector() connectors.Connector {
	return s.dest.PostgresConnector
}

func (s MySQLPGSuite) Peer() *protos.Peer {
	return GeneratePostgresPeer(s.t)
}

func (s MySQLPGSuite) DestinationTable(table string) string {
	return AttachSchema(s, table)
}

func (s MySQLPGSuite) GetRows(table string, cols string) (*model.QRecordBatch, error) {
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

func (s MySQLPGSuite) Teardown(ctx context.Context) {
	s.source.Teardown(s.t, ctx, s.suffix)
	TearDownPostgres(ctx, s)
}

func SetupMySQLPGSuite(t *testing.T) MySQLPGSuite {
	t.Helper()

	suffix := "mypg_" + strings.ToLower(common.RandomString(8))
	source, err := SetupMySQL(t, suffix)
	require.NoError(t, err, "failed to setup mysql")
	dest, err := SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	return MySQLPGSuite{t: t, source: source, dest: dest, suffix: suffix}
}

// The generic tests (Test_Simple_Flow, ...) with a MySQL source and a Postgres destination.
func TestGenericMySQLPG(t *testing.T) {
	e2eshared.RunSuite(t, SetupGenericSuite(SetupMySQLPGSuite))
}

func TestMySQLPG(t *testing.T) {
	e2eshared.RunSuite(t, SetupMySQLPGSuite)
}

// destInt runs a query returning one integer on the destination. Check values with direct SQL in this
// suite: the table-equality helpers treat NULL, empty strings and empty bytes as equal (qvalue.Equals) and decode
// the source with the same code as the snapshot, so empty-vs-NULL and decoding bugs pass them.
func (s MySQLPGSuite) destInt(query string) int64 {
	s.t.Helper()
	var n int64
	require.NoError(s.t, s.dest.Conn().QueryRow(s.t.Context(), query).Scan(&n), query)
	return n
}

// Canary: snapshot plus CDC insert / update / delete with types that replicate correctly on v0.37.10.
// It should pass on every build; if it fails, suspect the environment before the code under test.
// (TestGenericMySQLPG/Test_Simple_Flow is not a canary here: its SERIAL key is BIGINT UNSIGNED, which
// maps to bigint (identity-compatible; numeric(20,0) is the user's choice, see mysql_pg_unsigned_test.go),
// and the table comparison rejects UInt64 vs Int64. It fails by design while that default stands.)
func (s MySQLPGSuite) Test_Canary_Snapshot_And_CDC() {
	t := s.t
	srcTable := "canary"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(50), note TEXT, amount DECIMAL(10,2))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES (1, 'one', 'snapshot', 1.50), (2, 'two', NULL, 2.00)`, srcFull)))

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
	EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, srcTable, "id,name,note,amount")

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (3, 'three', 'cdc', 3.25)`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET note = 'updated' WHERE id = 1`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE id = 2`, srcFull)))
	EnvWaitForEqualTablesWithNames(env, s, "cdc insert/update/delete", srcTable, srcTable, "id,name,note,amount")
	EnvWaitFor(t, env, time.Minute, "direct check", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id = 1 AND note = 'updated') OR id = 3`, dstFull)) == 2 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// A primary-key UPDATE during CDC must remove the row under its old key, not leave it next to the new
// one. Fails on v0.37.10.
func (s MySQLPGSuite) Test_PKey_Update_Removes_Old_Row() {
	t := s.t
	srcTable := "pk_probe"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')`, srcFull)))

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
	EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, srcTable, "id,v")

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET id = 101 WHERE id = 1`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET id = 102, v = 'b2' WHERE id = 2`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 'c2' WHERE id = 3`, srcFull)))

	EnvWaitFor(t, env, 3*time.Minute, "updated rows under their new keys", func() bool {
		return s.destInt(fmt.Sprintf(
			`SELECT count(*) FROM %s WHERE (id, v) IN ((101, 'a'), (102, 'b2'), (3, 'c2'))`, dstFull)) == 3
	})
	oldKeys := s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id IN (1, 2)`, dstFull))
	total := s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull))

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.EqualValues(t, 0, oldKeys, "rows under the old primary keys were left behind")
	require.EqualValues(t, 3, total, "destination row count")
}
