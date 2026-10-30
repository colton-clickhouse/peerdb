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
// arrives as [1,2,3], POINT ZM (1 2 3 4) as [1,2,3,4]. GEOS refused the 4 coordinates, and the decoder wrote NULL.
func TestChangefeedGeoDimensions(t *testing.T) {
	pg := newPgValues(shared.InternalVersion_Latest, protos.DBType_POSTGRES)
	geom := types.QField{Name: "g", Type: types.QValueKindGeometry, Nullable: true}
	for _, tc := range []struct {
		raw  string
		m    bool
		want string
	}{
		{`{"type":"Point","coordinates":[1,2,3,4]}`, false, "POINT ZM (1 2 3 4)"},
		{`{"type":"Point","coordinates":[1,2,3]}`, true, "POINT M (1 2 3)"},
		{`{"type":"Point","coordinates":[1,2,3]}`, false, "POINT Z (1 2 3)"},
		{`{"type":"Point","coordinates":[1.5,2]}`, true, "POINT (1.5 2)"},
		{`{"type":"LineString","coordinates":[[0,0,1],[1,1,2]]}`, true, "LINESTRING M (0 0 1, 1 1 2)"},
		{`{"type":"LineString","coordinates":[[0,0,1,5],[1,1,2,6]]}`, false, "LINESTRING ZM (0 0 1 5, 1 1 2 6)"},
		{`{"type":"Polygon","coordinates":[[[0,0,1],[1,0,1],[1,1,1],[0,0,1]]]}`, true,
			"POLYGON M ((0 0 1, 1 0 1, 1 1 1, 0 0 1))"},
		{`{"type":"MultiPoint","coordinates":[[1,2,3,4],[5,6,7,8]]}`, false, "MULTIPOINT ZM (1 2 3 4, 5 6 7 8)"},
		{`{"type":"MultiPolygon","coordinates":[[[[0,0,1,2],[1,0,1,2],[1,1,1,2],[0,0,1,2]]]]}`, false,
			"MULTIPOLYGON ZM (((0 0 1 2, 1 0 1 2, 1 1 1 2, 0 0 1 2)))"},
		{`{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2,3,4]},` +
			`{"type":"LineString","coordinates":[[0,0,1,5],[1,1,2,6]]}]}`, false,
			"GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4), LINESTRING ZM (0 0 1 5, 1 1 2 6))"},
	} {
		qv, err := changefeedGeoValue(geom, json.RawMessage(tc.raw), tc.m)
		require.NoError(t, err, tc.raw)
		require.Equal(t, types.QValueGeometry{Val: tc.want}, qv, tc.raw)
	}

	// through the decoder: unconstrained columns read 4 coordinates as ZM, geography keeps its default SRID
	qv, err := qvalueFromChangefeedJSON(geom, json.RawMessage(`{"type":"Point","coordinates":[1,2,3,4]}`), pg)
	require.NoError(t, err)
	require.Equal(t, types.QValueGeometry{Val: "POINT ZM (1 2 3 4)"}, qv)
	qv, err = qvalueFromChangefeedJSON(types.QField{Name: "g", Type: types.QValueKindGeography, Nullable: true},
		json.RawMessage(`{"type":"Point","coordinates":[1,2,3,4]}`), pg)
	require.NoError(t, err)
	require.Equal(t, types.QValueGeography{Val: "SRID=4326;POINT ZM (1 2 3 4)"}, qv)

	// what neither reads fails, classified, instead of becoming NULL
	for _, raw := range []string{`{"type":"Point","coordinates":[1,2,3,4,5]}`, `{"type":"Nope","coordinates":[1,2]}`,
		`{"type":"LineString","coordinates":[[0,0,1,5],[1,1]]}`} {
		_, err := qvalueFromChangefeedJSON(geom, json.RawMessage(raw), pg)
		var geoErr *exceptions.GeometryParseError
		require.ErrorAs(t, err, &geoErr, raw)
	}

	// mirrors from before InternalVersion_CockroachGeoDimensions keep the older fallback
	old := newPgValues(shared.InternalVersion_CockroachGeoDimensions-1, protos.DBType_POSTGRES)
	qv, err = qvalueFromChangefeedJSON(geom, json.RawMessage(`{"type":"Point","coordinates":[1,2,3,4]}`), old)
	require.NoError(t, err)
	require.Equal(t, types.QValueNull(types.QValueKindGeometry), qv)
}

// A column whose type has M reads the changefeed's 3 coordinates as XYM, through the row decoding.
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
	row := map[string]json.RawMessage{
		"id": json.RawMessage(`1`), "pm": json.RawMessage(`{"type":"Point","coordinates":[1,2,3]}`),
		"g": json.RawMessage(`{"type":"Point","coordinates":[1,2,3]}`),
	}
	items, err := changefeedRecordItems(row, schema, nil)
	require.NoError(t, err)
	require.Equal(t, types.QValueGeometry{Val: "SRID=4326;POINT M (1 2 3)"}, items.GetColumnValue("pm"))
	require.Equal(t, types.QValueGeometry{Val: "POINT Z (1 2 3)"}, items.GetColumnValue("g"))
}

func geoTypmod(t *testing.T, args string) int32 {
	t.Helper()
	typmod, ok := datatypes.MakeGeoTypmod(args)
	require.True(t, ok, args)
	return typmod
}
