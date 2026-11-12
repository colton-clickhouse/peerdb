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
	sql, err := generateCreateTableSQLForNormalizedTable(config, &common.QualifiedTable{Namespace: "s", Table: "t"}, schema)
	require.NoError(t, err)
	require.Contains(t, sql, `"u" bigint`)
	require.Contains(t, sql, `"d" interval`)
	require.Contains(t, sql, `"n" INTEGER`)

	schema.System = protos.TypeSystem_PG
	schema.Columns = []*protos.FieldDescription{{Name: "u", Type: "integer", TypeModifier: -1}}
	sql, err = generateCreateTableSQLForNormalizedTable(config, &common.QualifiedTable{Namespace: "s", Table: "t"}, schema)
	require.NoError(t, err)
	require.Contains(t, sql, `"u" integer`, "the PG type system keeps the source's types")
}

// Table creation refuses a destination_type its column can't have, as validation does: validation is skipped with
// skip_validation, and a boolean column there wasn't read as an integer by the source.
func TestCreateTableRefusesDestinationTypes(t *testing.T) {
	schema := &protos.TableSchema{
		System: protos.TypeSystem_Q,
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64), TypeModifier: -1},
			{Name: "b", Type: string(types.QValueKindBoolean), TypeModifier: -1},
			{Name: "n", Type: string(types.QValueKindInt32), TypeModifier: -1},
		},
	}
	config := &protos.SetupNormalizedTableBatchInput{TableMappings: []*protos.TableMapping{
		{DestinationTableIdentifier: "s.t", Columns: []*protos.ColumnSetting{
			{SourceName: "id", DestinationType: "bigint"},
			{SourceName: "b", DestinationType: "smallint"},
			{SourceName: "n", DestinationType: "BIGINT"},
		}},
	}}
	_, err := generateCreateTableSQLForNormalizedTable(config, &common.QualifiedTable{Namespace: "s", Table: "t"}, schema)
	require.ErrorContains(t, err, "id: bigint isn't a destination_type for its type int64 (choices: numeric)")
	require.ErrorContains(t, err, "b: only MySQL TINYINT(1) columns can be read as integers")
	require.NotContains(t, err.Error(), "n:", "a listed type in another case is accepted")
}

// A resync recreates the tables with the column settings' types, so it's validated against them too.
func TestValidateDestinationTypesOnResync(t *testing.T) {
	cfg := &protos.FlowConnectionConfigsCore{
		FlowJobName: "m",
		System:      protos.TypeSystem_Q,
		Resync:      true,
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier: "db.t", DestinationTableIdentifier: "s.t",
			Columns: []*protos.ColumnSetting{{SourceName: "d", DestinationType: "bigint"}},
		}},
	}
	schemas := map[string]*protos.TableSchema{"db.t": {
		System:            protos.TypeSystem_Q,
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt32), TypeModifier: -1},
			{Name: "d", Type: string(types.QValueKindTime), TypeModifier: -1},
		},
	}}
	// refused before it reaches the destination
	err := (&PostgresConnector{}).ValidateMirrorDestination(t.Context(), cfg, schemas)
	require.ErrorContains(t, err, "db.t.d: bigint isn't a destination_type for its type time")
}

// Only an interval destination_type makes a column's intervals intervals; the type's case doesn't matter.
func TestIntervalColumns(t *testing.T) {
	require.Equal(t, map[string]struct{}{"i": {}, "a": {}}, intervalColumns([]*protos.ColumnSetting{
		{SourceName: "i", DestinationType: "interval"}, {SourceName: "a", DestinationType: "INTERVAL[]"},
		{SourceName: "t", DestinationType: "bigint"}, {SourceName: "n"},
	}))
	require.Nil(t, intervalColumns(nil))
}
