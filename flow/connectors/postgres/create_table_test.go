package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
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

// added columns are typed by the delta's version and source, as at creation; deltas without them keep the original
// mapping
func TestPgColumnTypeForKind(t *testing.T) {
	for _, tc := range []struct {
		kind    types.QValueKind
		version uint32
		source  protos.DBType
		want    string
	}{
		{types.QValueKindUInt32, shared.InternalVersion_Latest, protos.DBType_MYSQL, "BIGINT"},
		{types.QValueKindUInt32, shared.InternalVersion_First, protos.DBType_MYSQL, "INTEGER"},
		{types.QValueKindUInt16, shared.InternalVersion_Latest, protos.DBType_MYSQL, "INTEGER"},
		{types.QValueKindTime, shared.InternalVersion_Latest, protos.DBType_MYSQL, "INTERVAL"},
		{types.QValueKindTime, shared.InternalVersion_PgMySQLTimeAsInterval - 1, protos.DBType_MYSQL, "TIME"},
		{types.QValueKindTime, shared.InternalVersion_Latest, protos.DBType_POSTGRES, "TIME"},
		{types.QValueKindTime, 0, 0, "TIME"},
	} {
		require.Equal(t, tc.want, pgColumnTypeForKind(string(tc.kind), tc.version, tc.source), "%s %d %s",
			tc.kind, tc.version, tc.source)
	}
}
