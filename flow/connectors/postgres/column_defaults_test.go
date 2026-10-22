package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestPgDefaultExpr(t *testing.T) {
	for _, tc := range []struct {
		expr   string
		qkind  types.QValueKind
		pgType string
		want   string // "" for none
	}{
		{"5", types.QValueKindInt32, "INTEGER", "5"},
		{"'x'", types.QValueKindString, "TEXT", "'x'"},
		{"1", types.QValueKindBoolean, "BOOLEAN", "true"},
		{"0", types.QValueKindBoolean, "BOOLEAN", "false"},
		{"false", types.QValueKindBoolean, "BOOLEAN", "false"},
		{"'yes'", types.QValueKindBoolean, "BOOLEAN", ""},
		{"CURRENT_TIMESTAMP", types.QValueKindTimestamp, "TIMESTAMP", "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')"},
		{"CURRENT_TIMESTAMP", types.QValueKindTimestampTZ, "TIMESTAMPTZ", "CURRENT_TIMESTAMP"},
		{"CURRENT_TIMESTAMP", types.QValueKindString, "TEXT", ""},
		{"'2020-01-02 03:04:05'", types.QValueKindTimestamp, "TIMESTAMP", "'2020-01-02 03:04:05'"},
		{"'0000-00-00 00:00:00'", types.QValueKindTimestamp, "TIMESTAMP", ""},
		{"'2020-00-10'", types.QValueKindDate, "DATE", ""},
		{"'2020-10-00'", types.QValueKindDate, "DATE", ""},
		{"'12:30:00'", types.QValueKindTime, "TIME", "'12:30:00'"},
		{"'100:00:00'", types.QValueKindTime, "TIME", ""},
		{"'100:00:00'", types.QValueKindTime, "INTERVAL", "'100:00:00'"},
		{"'abc'", types.QValueKindBytes, "BYTEA", ""},
	} {
		got, ok := pgDefaultExpr(tc.expr, tc.qkind, tc.pgType)
		require.Equal(t, tc.want != "", ok, "%s %s", tc.expr, tc.qkind)
		require.Equal(t, tc.want, got, "%s %s", tc.expr, tc.qkind)
	}
}

// defaults are created from InternalVersion_PgColumnDefaults, with NOT NULL when nullability is propagated
func TestCreateTableColumnDefaults(t *testing.T) {
	five, x, now := "5", "'x'", "CURRENT_TIMESTAMP"
	schema := &protos.TableSchema{
		System:          protos.TypeSystem_Q,
		NullableEnabled: true,
		Columns: []*protos.FieldDescription{
			{Name: "qty", Type: string(types.QValueKindInt32), TypeModifier: -1, DefaultExpr: &five},
			{Name: "note", Type: string(types.QValueKindString), TypeModifier: -1, Nullable: true, DefaultExpr: &x},
			{Name: "at", Type: string(types.QValueKindTimestamp), TypeModifier: -1, Nullable: true, DefaultExpr: &now},
		},
	}
	dst := &common.QualifiedTable{Namespace: "s", Table: "t"}
	sql := generateCreateTableSQLForNormalizedTable(
		&protos.SetupNormalizedTableBatchInput{Version: shared.InternalVersion_PgColumnDefaults}, dst, schema)
	require.Contains(t, sql, `"qty" INTEGER NOT NULL DEFAULT 5`)
	require.Contains(t, sql, `"note" TEXT DEFAULT 'x'`)
	require.Contains(t, sql, `"at" TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')`)

	sql = generateCreateTableSQLForNormalizedTable(
		&protos.SetupNormalizedTableBatchInput{Version: shared.InternalVersion_PgColumnDefaults - 1}, dst, schema)
	require.NotContains(t, sql, "DEFAULT 5")
	require.NotContains(t, sql, "DEFAULT 'x'")
}
