package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// An array primary key (TypeSystem_Q) is partitioned by the column's own expression, not a cast of the raw JSON
// text, which is a JSON array: ('["10.0.0.1"]')::INET[] fails.
func TestGenerateMergeStatementArrayPrimaryKey(t *testing.T) {
	schema := &protos.TableSchema{
		TableIdentifier:   "test_table",
		System:            protos.TypeSystem_Q,
		PrimaryKeyColumns: []string{"a"},
		Columns: []*protos.FieldDescription{
			{Name: "a", Type: string(types.QValueKindArrayString)},
			{Name: "n", Type: string(types.QValueKindInt64)},
		},
	}
	gen := normalizeStmtGenerator{
		rawTableName:             "_peerdb_raw_test",
		tableSchemaMapping:       map[string]*protos.TableSchema{"public.test_table": schema},
		unchangedToastColumnsMap: map[string][]string{"public.test_table": {""}},
		peerdbCols:               &protos.PeerDBColumns{},
		metadataSchema:           "_peerdb_internal",
		supportsMerge:            true,
	}
	result := normalizeSQL(gen.generateMergeStatement("public.test_table", schema, []string{""}))
	require.Contains(t, result, normalizeSQL(`PARTITION BY CASE WHEN _peerdb_data->>'a' IS NULL THEN NULL`+
		` ELSE ARRAY(SELECT JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>'a')::JSON))::TEXT[] END`))
	require.NotContains(t, result, normalizeSQL(`(_peerdb_data->>'a')::TEXT[]`))

	fallback := normalizeSQL(gen.generateFallbackStatements("public.test_table", schema)[0])
	require.NotContains(t, fallback, normalizeSQL(`(_peerdb_data->>'a')::TEXT[]`))
}
