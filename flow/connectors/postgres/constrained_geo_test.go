package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// constrained geometry / geography columns are created with their PostGIS typmod.
func TestGenerateCreateTableConstrainedGeo(t *testing.T) {
	schema := &protos.TableSchema{
		System: protos.TypeSystem_Q,
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: "int64", TypeModifier: -1},
			{Name: "p", Type: "geometry", TypeModifier: 1107460},
			{Name: "g", Type: "geography", TypeModifier: 8},
			{Name: "u", Type: "geometry", TypeModifier: -1},
		},
		PrimaryKeyColumns: []string{"id"},
	}
	table := &common.QualifiedTable{Namespace: "s", Table: "t"}
	sql := generateCreateTableSQLForNormalizedTable(
		&protos.SetupNormalizedTableBatchInput{Version: shared.InternalVersion_PgConstrainedGeo}, table, schema)
	require.Contains(t, sql, `"p" geometry(Point,4326)`)
	require.Contains(t, sql, `"g" geography(LineString)`)
	require.Contains(t, sql, `"u" GEOMETRY`)

	sql = generateCreateTableSQLForNormalizedTable(
		&protos.SetupNormalizedTableBatchInput{Version: shared.InternalVersion_PgConstrainedGeo - 1}, table, schema)
	require.Contains(t, sql, `"p" GEOMETRY`)
}
