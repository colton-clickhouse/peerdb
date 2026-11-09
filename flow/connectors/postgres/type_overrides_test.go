package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// A column setting's destination_type is the created column's type on the Q type system; other columns keep
// PeerDB's mapping, and other tables' settings don't apply.
func TestCreateTableDestinationTypes(t *testing.T) {
	schema := &protos.TableSchema{
		System: protos.TypeSystem_Q,
		Columns: []*protos.FieldDescription{
			{Name: "u", Type: string(types.QValueKindUInt32), TypeModifier: -1},
			{Name: "d", Type: string(types.QValueKindTime), TypeModifier: -1},
			{Name: "n", Type: string(types.QValueKindInt32), TypeModifier: -1},
		},
	}
	config := &protos.SetupNormalizedTableBatchInput{TableMappings: []*protos.TableMapping{
		{DestinationTableIdentifier: "s.other", Columns: []*protos.ColumnSetting{{SourceName: "n", DestinationType: "numeric"}}},
		{DestinationTableIdentifier: "s.t", Columns: []*protos.ColumnSetting{
			{SourceName: "u", DestinationType: "bigint"}, {SourceName: "d", DestinationType: "interval"},
		}},
	}}
	sql := generateCreateTableSQLForNormalizedTable(config, &common.QualifiedTable{Namespace: "s", Table: "t"}, schema)
	require.Contains(t, sql, `"u" bigint`)
	require.Contains(t, sql, `"d" interval`)
	require.Contains(t, sql, `"n" INTEGER`)

	schema.System = protos.TypeSystem_PG
	schema.Columns = []*protos.FieldDescription{{Name: "u", Type: "integer", TypeModifier: -1}}
	sql = generateCreateTableSQLForNormalizedTable(config, &common.QualifiedTable{Namespace: "s", Table: "t"}, schema)
	require.Contains(t, sql, `"u" integer`, "the PG type system keeps the source's types")
}
