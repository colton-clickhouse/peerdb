package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestColumnTypeSettingsError(t *testing.T) {
	schemas := map[string]*protos.TableSchema{"src.t": {Columns: []*protos.FieldDescription{
		{Name: "as_int", Type: string(types.QValueKindInt8)},
		{Name: "as_bool", Type: string(types.QValueKindBoolean)},
		{Name: "name", Type: string(types.QValueKindString)},
		{Name: "small", Type: string(types.QValueKindInt16)},
	}}}
	mapping := func(settings ...*protos.ColumnSetting) []*protos.TableMapping {
		return []*protos.TableMapping{{SourceTableIdentifier: "src.t", DestinationTableIdentifier: "dst.t", Columns: settings}}
	}

	require.NoError(t, columnTypeSettingsError(mapping(
		&protos.ColumnSetting{SourceName: "as_int", DestinationType: "smallint"},
		&protos.ColumnSetting{SourceName: "as_bool", DestinationType: "BOOLEAN"},
		&protos.ColumnSetting{SourceName: "name", DestinationName: "renamed"}, // no destination type
		// the type the column gets anyway: harmless, and accepted
		&protos.ColumnSetting{SourceName: "small", DestinationType: "int2"},
		&protos.ColumnSetting{SourceName: "name", DestinationType: "TEXT"},
	), schemas, shared.InternalVersion_Latest))

	err := columnTypeSettingsError(mapping(
		&protos.ColumnSetting{SourceName: "as_int", DestinationType: "integer"},
		&protos.ColumnSetting{SourceName: "as_bool", DestinationType: "smallint"}, // read as a boolean: not applied
		&protos.ColumnSetting{SourceName: "name", DestinationType: "varchar(10)"},
		&protos.ColumnSetting{SourceName: "missing", DestinationType: "text"}, // not a column of the source
	), schemas, shared.InternalVersion_Latest)
	require.ErrorContains(t, err,
		"src.t.as_int integer, src.t.as_bool smallint, src.t.name varchar(10), src.t.missing text")

	// resync doesn't read the source schema: the settings were checked when the table was added
	require.NoError(t, columnTypeSettingsError(mapping(
		&protos.ColumnSetting{SourceName: "as_int", DestinationType: "smallint"},
	), nil, shared.InternalVersion_Latest))
}
