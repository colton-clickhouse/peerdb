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
		// TINYINT(1) given as a string
		{"'1'", types.QValueKindBoolean, "BOOLEAN", "true"},
		// a Postgres source doubles backslashes, which Postgres would read literally
		{`'a\\b'`, types.QValueKindString, "TEXT", ""},
		// out of the created type's range: accepted by CREATE TABLE, refused by every insert that uses it
		{"18446744073709551615", types.QValueKindUInt64, "BIGINT", ""},
		{"4294967295", types.QValueKindUInt32, "BIGINT", "4294967295"},
		{"65535", types.QValueKindUInt16, "SMALLINT", ""},
		{"-32768", types.QValueKindInt16, "SMALLINT", "-32768"},
		{"18446744073709551615", types.QValueKindUInt64, "NUMERIC(20,0)", "18446744073709551615"},
		// impossible dates (MySQL's ALLOW_INVALID_DATES)
		{"'2020-02-31'", types.QValueKindDate, "DATE", ""},
		{"'2020-02-29 25:00:00'", types.QValueKindTimestamp, "TIMESTAMP", ""},
		{"'2020-02-29 23:59:59.123456'", types.QValueKindTimestamp, "TIMESTAMP", "'2020-02-29 23:59:59.123456'"},
		// a Postgres source gives timestamptz in UTC without an offset; the literal is coerced in PeerDB's UTC session
		{"'2020-01-02 03:04:05'", types.QValueKindTimestampTZ, "TIMESTAMPTZ", "'2020-01-02 03:04:05'"},
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

// A change synced before a column with a literal default existed doesn't carry it: an insert takes the default and
// an update keeps the row's value, instead of writing NULL. A time default isn't filled (normalize time isn't the
// source's).
func TestGenerateMergeStatement_MissingDefaultedColumn(t *testing.T) {
	const table = `"public"."t"`
	x, now := "'x'", "CURRENT_TIMESTAMP"
	schema := &protos.TableSchema{System: protos.TypeSystem_Q, PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt32), TypeModifier: -1},
			{Name: "note", Type: string(types.QValueKindString), TypeModifier: -1, Nullable: true, DefaultExpr: &x},
			{Name: "at", Type: string(types.QValueKindTimestamp), TypeModifier: -1, Nullable: true, DefaultExpr: &now},
		}}
	g := normalizeStmtGenerator{
		rawTableName:       "_peerdb_raw_test",
		tableSchemaMapping: map[string]*protos.TableSchema{table: schema},
		peerdbCols:         &protos.PeerDBColumns{},
		metadataSchema:     "_peerdb_internal",
		supportsMerge:      true,
	}
	stmt := g.generateMergeStatement(table, schema, []string{""})
	require.Contains(t, stmt, `(_peerdb_data ? 'note') AS "_peerdb_has_1"`)
	require.Contains(t, stmt, `CASE WHEN src."_peerdb_has_1" THEN src."note" ELSE 'x' END`)
	require.Contains(t, stmt, `"note"=CASE WHEN src."_peerdb_has_1" THEN src."note" ELSE dst."note" END`)
	require.NotContains(t, stmt, `_peerdb_has_2`, "no fill for a time default")
	require.Contains(t, stmt, `"at"=src."at"`)
}

// AUTO_INCREMENT columns become identities from InternalVersion_PgAutoIncrementIdentity, integer types only
func TestCreateTableAutoIncrementIdentity(t *testing.T) {
	schema := &protos.TableSchema{
		System:            protos.TypeSystem_Q,
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindUInt64), TypeModifier: -1, AutoIncrement: true},
			{Name: "seq", Type: string(types.QValueKindInt32), TypeModifier: -1, AutoIncrement: true},
			// an identity is NOT NULL: not for a column the source declares nullable
			{Name: "nseq", Type: string(types.QValueKindInt32), TypeModifier: -1, Nullable: true, AutoIncrement: true},
			{Name: "num", Type: string(types.QValueKindNumeric), TypeModifier: -1, Nullable: true, AutoIncrement: true},
		},
	}
	dst := &common.QualifiedTable{Namespace: "s", Table: "t"}
	sql := generateCreateTableSQLForNormalizedTable(
		&protos.SetupNormalizedTableBatchInput{Version: shared.InternalVersion_PgAutoIncrementIdentity}, dst, schema)
	require.Contains(t, sql, `"id" BIGINT GENERATED BY DEFAULT AS IDENTITY`)
	require.Contains(t, sql, `"seq" INTEGER GENERATED BY DEFAULT AS IDENTITY`)
	require.NotContains(t, sql, `"num" NUMERIC GENERATED`)
	require.NotContains(t, sql, `"nseq" INTEGER GENERATED`)

	sql = generateCreateTableSQLForNormalizedTable(
		&protos.SetupNormalizedTableBatchInput{Version: shared.InternalVersion_PgAutoIncrementIdentity - 1}, dst, schema)
	require.NotContains(t, sql, "IDENTITY")
}
