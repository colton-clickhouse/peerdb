package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestColumnTypeSettingsError(t *testing.T) {
	schemas := map[string]*protos.TableSchema{"src.t": {Columns: []*protos.FieldDescription{
		{Name: "as_int", Type: string(types.QValueKindInt8)},
		{Name: "as_bool", Type: string(types.QValueKindBoolean)},
		{Name: "name", Type: string(types.QValueKindString)},
	}}}
	mapping := func(settings ...*protos.ColumnSetting) []*protos.TableMapping {
		return []*protos.TableMapping{{SourceTableIdentifier: "src.t", DestinationTableIdentifier: "dst.t", Columns: settings}}
	}

	require.NoError(t, columnTypeSettingsError(mapping(
		&protos.ColumnSetting{SourceName: "as_int", DestinationType: "smallint"},
		&protos.ColumnSetting{SourceName: "as_bool", DestinationType: "BOOLEAN"},
		&protos.ColumnSetting{SourceName: "name", DestinationName: "renamed"}, // no destination type
	), schemas))

	err := columnTypeSettingsError(mapping(
		&protos.ColumnSetting{SourceName: "as_int", DestinationType: "integer"},
		&protos.ColumnSetting{SourceName: "as_bool", DestinationType: "smallint"}, // read as a boolean: not applied
		&protos.ColumnSetting{SourceName: "name", DestinationType: "text"},
	), schemas)
	require.ErrorContains(t, err, "src.t.as_int integer, src.t.as_bool smallint, src.t.name text")
}
