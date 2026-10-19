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
			{Name: "c_code", Type: string(types.QValueKindString), TypeModifier: -1},
			{Name: "c_dec", Type: string(types.QValueKindNumeric), TypeModifier: -1},
			{Name: "c_dbl", Type: string(types.QValueKindFloat64), TypeModifier: -1},
			{Name: "c_dec_text", Type: string(types.QValueKindNumeric), TypeModifier: -1},
		},
	}
	gen := func(targetTypes map[string]map[string]string, version uint32) string {
		g := normalizeStmtGenerator{
			rawTableName:       "_peerdb_raw_test",
			tableSchemaMapping: map[string]*protos.TableSchema{table: schema},
			peerdbCols:         &protos.PeerDBColumns{},
			metadataSchema:     "_peerdb_internal",
			supportsMerge:      true,
			version:            version,
			targetColumnTypes:  targetTypes,
		}
		return g.generateMergeStatement(table, schema, nil)
	}

	// casts follow the target: a column widened by hand (c_int), the NUMERIC(20,0) choice for BIGINT
	// UNSIGNED (c_big, cast without its modifier), bytea keeps its base64 decode; a column the target
	// doesn't report (c_text) falls back to PeerDB's mapping
	stmt := gen(map[string]map[string]string{table: {
		"id": "integer", "c_int": "bigint", "c_big": "numeric", "c_bytes": "bytea", "c_code": "bpchar",
		"c_dec": "integer", "c_dbl": "bigint", "c_dec_text": "text",
	}}, shared.InternalVersion_Latest)
	for _, want := range []string{
		`(_peerdb_data->>'id')::integer`,
		`(_peerdb_data->>'c_int')::bigint AS "c_int"`,
		`(_peerdb_data->>'c_big')::numeric AS "c_big"`,
		`decode(_peerdb_data->>'c_bytes', 'base64')::bytea AS "c_bytes"`,
		`(_peerdb_data->>'c_text')::TEXT AS "c_text"`,
		// char(n) as bpchar: casting to "character" would mean char(1) and truncate
		`(_peerdb_data->>'c_code')::bpchar AS "c_code"`,
		// decimal and float text isn't integer input: through PeerDB's type, rounded on the way to the target
		`(_peerdb_data->>'c_dec')::NUMERIC::integer AS "c_dec"`,
		`(_peerdb_data->>'c_dbl')::DOUBLE PRECISION::bigint AS "c_dbl"`,
		`(_peerdb_data->>'c_dec_text')::text AS "c_dec_text"`,
	} {
		require.Contains(t, stmt, want)
	}

	// without target types (older mirrors): PeerDB's mapped types, as before
	stmt = gen(nil, shared.InternalVersion_PgWidenUnsignedInts-1)
	require.Contains(t, stmt, `(_peerdb_data->>'c_int')::INTEGER AS "c_int"`)
	require.Contains(t, stmt, `(_peerdb_data->>'c_big')::BIGINT AS "c_big"`)
}

// Which values go through PeerDB's own type on the way to the target column's type, and which are cast from
// their text directly.
func TestGenerateMergeStatement_CastRules(t *testing.T) {
	const table = `"public"."t"`
	columns := []struct{ name, kind, target, want string }{
		// directly: PeerDB's own type would be narrower than the value
		{"c_time", string(types.QValueKindTime), "interval", `(_peerdb_data->>'c_time')::interval AS "c_time"`},
		{"c_u64", string(types.QValueKindUInt64), "numeric", `(_peerdb_data->>'c_u64')::numeric AS "c_u64"`},
		{"c_enum", string(types.QValueKindString), "mood", `(_peerdb_data->>'c_enum')::mood AS "c_enum"`},
		{"c_own", string(types.QValueKindFloat64), "double precision",
			`(_peerdb_data->>'c_own')::double precision AS "c_own"`},
		// through PeerDB's type: floats as the snapshot writes them, numeric into integers rounds
		{"c_f32", string(types.QValueKindFloat32), "double precision",
			`(_peerdb_data->>'c_f32')::REAL::double precision AS "c_f32"`},
		{"c_f64", string(types.QValueKindFloat64), "numeric", `(_peerdb_data->>'c_f64')::DOUBLE PRECISION::numeric AS "c_f64"`},
		{"c_dec", string(types.QValueKindNumeric), "integer", `(_peerdb_data->>'c_dec')::NUMERIC::integer AS "c_dec"`},
		// arrays: text[] has no cast to a vector or from float text to integer[]
		{"c_vec", string(types.QValueKindArrayFloat32), "vector",
			`CASE WHEN _peerdb_data->>'c_vec' IS NULL THEN NULL ELSE ARRAY(SELECT JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>'c_vec')::JSON))::REAL[]::vector END AS "c_vec"`},
		{"c_ints", string(types.QValueKindArrayFloat64), "integer[]",
			`CASE WHEN _peerdb_data->>'c_ints' IS NULL THEN NULL ELSE ARRAY(SELECT JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>'c_ints')::JSON))::DOUBLE PRECISION[]::integer[] END AS "c_ints"`},
		// into bytea: the text's bytes, not its backslash escapes; a UUID's 16 bytes
		{"c_str_bytes", string(types.QValueKindString), "bytea", `convert_to(_peerdb_data->>'c_str_bytes', 'UTF8') AS "c_str_bytes"`},
		{"c_uuid_bytes", string(types.QValueKindUUID), "bytea", `uuid_send((_peerdb_data->>'c_uuid_bytes')::uuid) AS "c_uuid_bytes"`},
	}
	schema := &protos.TableSchema{System: protos.TypeSystem_Q, PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{{Name: "id", Type: string(types.QValueKindInt32), TypeModifier: -1}}}
	targets := map[string]string{"id": "integer"}
	for _, c := range columns {
		schema.Columns = append(schema.Columns, &protos.FieldDescription{Name: c.name, Type: c.kind, TypeModifier: -1})
		targets[c.name] = c.target
	}
	g := normalizeStmtGenerator{
		rawTableName:       "_peerdb_raw_test",
		tableSchemaMapping: map[string]*protos.TableSchema{table: schema},
		peerdbCols:         &protos.PeerDBColumns{},
		metadataSchema:     "_peerdb_internal",
		supportsMerge:      true,
		version:            shared.InternalVersion_Latest,
		targetColumnTypes:  map[string]map[string]string{table: targets},
	}
	stmt := g.generateMergeStatement(table, schema, nil)
	for _, c := range columns {
		require.Contains(t, stmt, c.want, c.name)
	}
	require.NotContains(t, stmt, "::TIME::", "TIME into interval must be cast directly")

	// Postgres before 15 (no MERGE): the same casts in the upsert
	g.supportsMerge = false
	fallback := g.generateFallbackStatements(table, schema)
	require.Contains(t, fallback[0], `(_peerdb_data->>'c_dec')::NUMERIC::integer AS "c_dec"`)
	require.Contains(t, fallback[0], `(_peerdb_data->>'c_time')::interval AS "c_time"`)
}

// Target column types are read only from InternalVersion_PgNormalizeCastToTargetTypes on, not by mirrors that
// only widen unsigned integers.
func TestReadsTargetColumnTypes(t *testing.T) {
	require.False(t, readsTargetColumnTypes(shared.InternalVersion_PgWidenUnsignedInts))
	require.True(t, readsTargetColumnTypes(shared.InternalVersion_PgNormalizeCastToTargetTypes))
	require.True(t, readsTargetColumnTypes(shared.InternalVersion_Latest))
}
