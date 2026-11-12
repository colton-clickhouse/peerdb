package conncockroachdb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Changefeed GeoJSON as CockroachDB emits it for values with an M dimension (captured on v25.4): POINT M (1 2 3)
// arrives as [1,2,3], POINT ZM (1 2 3 4) as [1,2,3,4]. GEOS refuses 4 coordinates, and the decoder used to write NULL
// for them: now it's a classified error, on every destination.
func TestChangefeedGeoDimensions(t *testing.T) {
	pg := newPgValues(shared.InternalVersion_Latest, protos.DBType_POSTGRES)
	geom := types.QField{Name: "g", Type: types.QValueKindGeometry, Nullable: true}
	for _, field := range []types.QField{geom, {Name: "g", Type: types.QValueKindGeography, Nullable: true}} {
		for _, raw := range []string{`{"type":"Point","coordinates":[1,2,3,4]}`,
			`{"type":"LineString","coordinates":[[0,0,1,5],[1,1,2,6]]}`, `{"type":"Nope","coordinates":[1,2]}`} {
			_, err := qvalueFromChangefeedJSON(field, json.RawMessage(raw), pg)
			var geoErr *exceptions.GeometryParseError
			require.ErrorAs(t, err, &geoErr, "%s %s", field.Type, raw)
		}
	}
	// 2 and 3 coordinates in a column without M read as before
	qv, err := qvalueFromChangefeedJSON(geom, json.RawMessage(`{"type":"Point","coordinates":[1,2,3]}`), pg)
	require.NoError(t, err)
	require.Equal(t, types.QValueGeometry{Val: "POINT Z (1 2 3)"}, qv)
	qv, err = qvalueFromChangefeedJSON(geom, json.RawMessage(`{"type":"Point","coordinates":[1.5,2]}`), pg)
	require.NoError(t, err)
	require.Equal(t, types.QValueGeometry{Val: "POINT (1.5 2)"}, qv)
}

// A column whose type has M but no Z fails CDC rather than reading the changefeed's 3 coordinates as XYZ; NULL is fine,
// and a column without M next to it reads as before.
func TestChangefeedGeoMColumn(t *testing.T) {
	schema := newChangefeedTableSchema(&protos.TableSchema{
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
			{Name: "pm", Type: string(types.QValueKindGeometry), TypeModifier: geoTypmod(t, "PointM,4326"), Nullable: true},
			{Name: "g", Type: string(types.QValueKindGeometry), TypeModifier: -1, Nullable: true},
		},
	}, nil)
	schema.pg = newPgValues(shared.InternalVersion_Latest, protos.DBType_POSTGRES)
	_, err := changefeedRecordItems(map[string]json.RawMessage{
		"id": json.RawMessage(`1`), "pm": json.RawMessage(`{"type":"Point","coordinates":[1,2,3]}`),
	}, schema, nil)
	var geoErr *exceptions.GeometryParseError
	require.ErrorAs(t, err, &geoErr)
	require.ErrorContains(t, err, "column pm")

	items, err := changefeedRecordItems(map[string]json.RawMessage{
		"id": json.RawMessage(`1`), "pm": json.RawMessage(`null`),
		"g": json.RawMessage(`{"type":"Point","coordinates":[1,2,3]}`),
	}, schema, nil)
	require.NoError(t, err)
	require.Equal(t, types.QValueNull(types.QValueKindGeometry), items.GetColumnValue("pm"))
	require.Equal(t, types.QValueGeometry{Val: "POINT Z (1 2 3)"}, items.GetColumnValue("g"))
}

func geoTypmod(t *testing.T, args string) int32 {
	t.Helper()
	typmod, ok := datatypes.MakeGeoTypmod(args)
	require.True(t, ok, args)
	return typmod
}
