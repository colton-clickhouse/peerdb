package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

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
			{Name: "c_dec_scaled", Type: string(types.QValueKindNumeric), TypeModifier: datatypes.MakeNumericTypmod(12, 3)},
			{Name: "c_code_domain", Type: string(types.QValueKindString), TypeModifier: -1},
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
		"id": "integer", "c_int": "bigint", "c_big": "numeric", "c_bytes": "bytea", "c_code": "bpchar",
		"c_dec": "integer", "c_dbl": "bigint", "c_dec_text": "text", "c_dec_scaled": "text",
		"c_code_domain": "character varying",
	}})
	for _, want := range []string{
		`(_peerdb_data->>'id')::integer`,
		`(_peerdb_data->>'c_int')::bigint AS "c_int"`,
		`(_peerdb_data->>'c_big')::numeric AS "c_big"`,
		`decode(_peerdb_data->>'c_bytes', 'base64')::BYTEA AS "c_bytes"`,
		`(_peerdb_data->>'c_text')::TEXT AS "c_text"`,
		// char(n) as bpchar: casting to "character" would mean char(1) and truncate; a string target, so through
		// PeerDB's type
		`(_peerdb_data->>'c_code')::TEXT::bpchar AS "c_code"`,
		// decimal and float text isn't integer input: through PeerDB's type, rounded on the way to the target
		`(_peerdb_data->>'c_dec')::NUMERIC::integer AS "c_dec"`,
		`(_peerdb_data->>'c_dbl')::DOUBLE PRECISION::bigint AS "c_dbl"`,
		// string targets: through PeerDB's type, so the text is what assignment gave before
		`(_peerdb_data->>'c_dec_text')::NUMERIC::text AS "c_dec_text"`,
		// without the source's scale: the raw JSON holds 1234.5, as the snapshot COPY writes it into text
		`(_peerdb_data->>'c_dec_scaled')::NUMERIC::text AS "c_dec_scaled"`,
		// a domain over varchar(n) is read as its base type: cast to that, so assignment to the domain refuses
		// an over-long value (a cast to the domain truncates it)
		`(_peerdb_data->>'c_code_domain')::TEXT::character varying AS "c_code_domain"`,
	} {
		require.Contains(t, stmt, want)
	}

	// without target types (a table normalize can't find): PeerDB's mapped types, as before
	stmt = gen(nil)
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
		// string targets: through PeerDB's type, as assignment converted them before
		{"c_ts_text", string(types.QValueKindTimestamp), "text",
			`(_peerdb_data->>'c_ts_text')::TIMESTAMP::text AS "c_ts_text"`},
		{"c_json_varchar", string(types.QValueKindJSON), "character varying",
			`(_peerdb_data->>'c_json_varchar')::JSON::character varying AS "c_json_varchar"`},
		{"c_str_text", string(types.QValueKindString), "text", `(_peerdb_data->>'c_str_text')::text AS "c_str_text"`},
		// into bytea: PeerDB's type, as before (text::bytea would read backslash escapes; text into bytea fails)
		{"c_str_bytes", string(types.QValueKindString), "bytea", `(_peerdb_data->>'c_str_bytes')::TEXT AS "c_str_bytes"`},
		{"c_uuid_bytes", string(types.QValueKindUUID), "bytea", `(_peerdb_data->>'c_uuid_bytes')::UUID AS "c_uuid_bytes"`},
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

// A string key into a bytea target keeps PeerDB's type in the value and in the PARTITION BY, as before: text has
// no assignment cast to bytea, so normalize fails loudly instead of storing the text's backslash escapes.
func TestGenerateMergeStatement_StringKeyIntoBytea(t *testing.T) {
	const table = `"public"."t"`
	schema := &protos.TableSchema{
		System:            protos.TypeSystem_Q,
		PrimaryKeyColumns: []string{"code"},
		Columns:           []*protos.FieldDescription{{Name: "code", Type: string(types.QValueKindString), TypeModifier: -1}},
	}
	g := normalizeStmtGenerator{
		rawTableName:       "_peerdb_raw_test",
		tableSchemaMapping: map[string]*protos.TableSchema{table: schema},
		peerdbCols:         &protos.PeerDBColumns{},
		metadataSchema:     "_peerdb_internal",
		supportsMerge:      true,
		targetColumnTypes:  map[string]map[string]string{table: {"code": "bytea"}},
	}
	stmt := g.generateMergeStatement(table, schema, nil)
	require.Contains(t, stmt, `PARTITION BY (_peerdb_data->>'code')::TEXT ORDER BY`)
	require.Contains(t, stmt, `(_peerdb_data->>'code')::TEXT AS "code"`)
	require.NotContains(t, stmt, "bytea")
}
