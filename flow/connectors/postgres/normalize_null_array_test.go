package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestGenerateExprNullArray(t *testing.T) {
	schema := &protos.TableSchema{System: protos.TypeSystem_Q}
	arr := string(types.QValueKindArrayInt64)

	old := &normalizeStmtGenerator{version: shared.InternalVersion_PgNullArrayStaysNull - 1}
	require.Equal(t, `ARRAY(SELECT JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>'a')::JSON))::BIGINT[]`,
		old.generateExpr(schema, arr, `'a'`, "BIGINT[]"))

	cur := &normalizeStmtGenerator{version: shared.InternalVersion_PgNullArrayStaysNull}
	require.Equal(t, `CASE WHEN _peerdb_data->>'a' IS NULL THEN NULL`+
		` ELSE ARRAY(SELECT JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>'a')::JSON))::BIGINT[] END`,
		cur.generateExpr(schema, arr, `'a'`, "BIGINT[]"))

	// scalars are unchanged by the gate
	require.Equal(t, `(_peerdb_data->>'i')::BIGINT`, cur.generateExpr(schema, string(types.QValueKindInt64), `'i'`, "BIGINT"))
}
