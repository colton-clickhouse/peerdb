package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// MySQL TIME is created as INTERVAL from InternalVersion_PgMySQLTimeAsInterval; other sources' TIME (a time
// of day, e.g. CockroachDB) and older mirrors keep TIME
func TestCreateTableTimeColumnBySource(t *testing.T) {
	schema := &protos.TableSchema{
		System:  protos.TypeSystem_Q,
		Columns: []*protos.FieldDescription{{Name: "c_time", Type: "time", TypeModifier: -1, Nullable: true}},
	}
	dst := &common.QualifiedTable{Namespace: "s", Table: "t"}
	for _, tc := range []struct {
		source  protos.DBType
		version uint32
		want    string
	}{
		{protos.DBType_MYSQL, shared.InternalVersion_PgMySQLTimeAsInterval, `"c_time" INTERVAL`},
		{protos.DBType_MYSQL, shared.InternalVersion_PgMySQLTimeAsInterval - 1, `"c_time" TIME`},
		{protos.DBType_COCKROACHDB, shared.InternalVersion_PgMySQLTimeAsInterval, `"c_time" TIME`},
	} {
		sql := generateCreateTableSQLForNormalizedTable(
			&protos.SetupNormalizedTableBatchInput{SourcePeerType: tc.source, Version: tc.version}, dst, schema)
		require.Contains(t, sql, tc.want, "%s v%d: %s", tc.source, tc.version, sql)
	}
}
