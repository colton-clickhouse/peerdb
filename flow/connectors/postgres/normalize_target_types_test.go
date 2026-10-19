package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestQValueKindToPostgresType_UnsignedWidening(t *testing.T) {
	before := shared.InternalVersion_PgWidenUnsignedInts - 1
	for _, tc := range []struct {
		kind             types.QValueKind
		wantOld, wantNew string
	}{
		{types.QValueKindUInt8, "SMALLINT", "SMALLINT"},
		{types.QValueKindUInt16, "SMALLINT", "INTEGER"},
		{types.QValueKindUInt32, "INTEGER", "BIGINT"},
		// NUMERIC(20,0) would be lossless but can't be an identity column: the user's choice
		{types.QValueKindUInt64, "BIGINT", "BIGINT"},
		{types.QValueKindInt16, "SMALLINT", "SMALLINT"},
		{types.QValueKindInt32, "INTEGER", "INTEGER"},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			require.Equal(t, tc.wantOld, qValueKindToPostgresType(string(tc.kind), before))
			require.Equal(t, tc.wantNew, qValueKindToPostgresType(string(tc.kind), shared.InternalVersion_PgWidenUnsignedInts))
		})
	}
}

func TestGenerateMergeStatement_TargetColumnTypes(t *testing.T) {
	const table = `"public"."t"`
	schema := &protos.TableSchema{
		System:            protos.TypeSystem_Q,
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt32), TypeModifier: -1},
			{Name: "c_int", Type: string(types.QValueKindUInt32), TypeModifier: -1},
			{Name: "c_big", Type: string(types.QValueKindUInt64), TypeModifier: -1},
			{Name: "c_bytes", Type: string(types.QValueKindBytes), TypeModifier: -1},
			{Name: "c_text", Type: string(types.QValueKindString), TypeModifier: -1},
		},
	}
	gen := func(targetTypes map[string]map[string]string) string {
		g := normalizeStmtGenerator{
			rawTableName:       "_peerdb_raw_test",
			tableSchemaMapping: map[string]*protos.TableSchema{table: schema},
			peerdbCols:         &protos.PeerDBColumns{},
			metadataSchema:     "_peerdb_internal",
			supportsMerge:      true,
			targetColumnTypes:  targetTypes,
		}
		return g.generateMergeStatement(table, schema, nil)
	}

	// casts follow the target: a column widened by hand (c_int), the NUMERIC(20,0) choice for BIGINT
	// UNSIGNED (c_big, cast without its modifier), bytea keeps its base64 decode; a column the target
	// doesn't report (c_text) falls back to PeerDB's mapping
	stmt := gen(map[string]map[string]string{table: {
		"id": "integer", "c_int": "bigint", "c_big": "numeric", "c_bytes": "bytea",
	}})
	for _, want := range []string{
		`(_peerdb_data->>'id')::integer`,
		`(_peerdb_data->>'c_int')::bigint AS "c_int"`,
		`(_peerdb_data->>'c_big')::numeric AS "c_big"`,
		`decode(_peerdb_data->>'c_bytes', 'base64')::bytea AS "c_bytes"`,
		`(_peerdb_data->>'c_text')::TEXT AS "c_text"`,
	} {
		require.Contains(t, stmt, want)
	}

	// without target types (older mirrors): PeerDB's mapped types, as before
	stmt = gen(nil)
	require.Contains(t, stmt, `(_peerdb_data->>'c_int')::INTEGER AS "c_int"`)
	require.Contains(t, stmt, `(_peerdb_data->>'c_big')::BIGINT AS "c_big"`)
}
