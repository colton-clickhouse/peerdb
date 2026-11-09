//go:build tilt

package mysql_postgres

// MySQL source -> Postgres destination. Upstream has no such suite: PeerFlowE2ETestSuitePG is Postgres -> Postgres and
// the MySQL suites target ClickHouse. The MySQL flavor comes from CI_MYSQL_* like the other MySQL suites.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type MySQLPGSuite struct {
	t      *testing.T
	source *e2e.MySqlSource
	dest   *e2e.PostgresSource
	suffix string
}

func (s MySQLPGSuite) T() *testing.T {
	return s.t
}

func (s MySQLPGSuite) Suffix() string {
	return s.suffix
}

func (s MySQLPGSuite) Source() e2e.SuiteSource {
	return s.source
}

func (s MySQLPGSuite) Connector() *connpostgres.PostgresConnector {
	return s.dest.PostgresConnector
}

func (s MySQLPGSuite) DestinationConnector() connectors.Connector {
	return s.dest.PostgresConnector
}

func (s MySQLPGSuite) Peer() *protos.Peer {
	return e2e.GeneratePostgresPeer(s.t)
}

func (s MySQLPGSuite) DestinationTable(table string) string {
	return e2e.AttachSchema(s, table)
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
	e2e.TearDownPostgres(ctx, s)
}

func SetupMySQLPGSuite(t *testing.T) MySQLPGSuite {
	t.Helper()

	suffix := "mypg_" + strings.ToLower(common.RandomString(8))
	source, err := e2e.SetupMySQL(t, suffix)
	require.NoError(t, err, "failed to setup mysql")
	dest, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	return MySQLPGSuite{t: t, source: source, dest: dest, suffix: suffix}
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

// Canary: snapshot plus CDC insert / update / delete with types that replicate correctly without this stack's
// fixes. It should pass on every build; if it fails, suspect the environment before the code under test.
func (s MySQLPGSuite) Test_Canary_Snapshot_And_CDC() {
	t := s.t
	srcTable := "canary"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(50), note TEXT, amount DECIMAL(10,2))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES (1, 'one', 'snapshot', 1.50), (2, 'two', NULL, 2.00)`, srcFull)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t, tc, flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, srcTable, "id,name,note,amount")

	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (3, 'three', 'cdc', 3.25)`, srcFull)))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET note = 'updated' WHERE id = 1`, srcFull)))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE id = 2`, srcFull)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "cdc insert/update/delete", srcTable, srcTable, "id,name,note,amount")
	e2e.EnvWaitFor(t, env, time.Minute, "direct check", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id = 1 AND note = 'updated') OR id = 3`, dstFull)) == 2 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})

	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}
