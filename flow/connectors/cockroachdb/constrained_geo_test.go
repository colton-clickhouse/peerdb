package conncockroachdb

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// information_schema.columns.data_type of constrained columns, as CockroachDB 25.4 reports them.
func TestConstrainedGeoTypmod(t *testing.T) {
	v := shared.InternalVersion_PgConstrainedGeo
	for _, tc := range []struct {
		dataType, udtName string
		typmod            int32
	}{
		{"geometry(point,4326)", "geometry", 1107460},
		{"geometry(Polygon)", "geometry", 12},
		{"geography(point,4326)", "geography", 1107460},
		{"geography(LineString)", "geography", 8},
		{"geometry(pointz,3857)", "geometry", 987398},
		{"geometry(PointM)", "geometry", 5},
	} {
		typmod, ok := constrainedGeoTypmod(tc.dataType, tc.udtName, v)
		require.True(t, ok, tc.dataType)
		require.Equal(t, tc.typmod, typmod, tc.dataType)
	}
	for _, tc := range [][2]string{{"geometry", "geometry"}, {"text", "text"}, {"geometry(circle)", "geometry"}} {
		_, ok := constrainedGeoTypmod(tc[0], tc[1], v)
		require.False(t, ok, tc[0])
	}
	_, ok := constrainedGeoTypmod("geometry(point,4326)", "geometry", v-1)
	require.False(t, ok, "older mirrors keep text")
}

func TestWithGeoSRID(t *testing.T) {
	require.Equal(t, types.QValueGeometry{Val: "SRID=3857;POINT (1 2)"}, withGeoSRID(types.QValueGeometry{Val: "POINT (1 2)"}, 3857))
	// the changefeed path defaults geography to 4326; the column's SRID wins
	require.Equal(t, types.QValueGeography{Val: "SRID=4269;POINT (1 2)"},
		withGeoSRID(types.QValueGeography{Val: "SRID=4326;POINT (1 2)"}, 4269))
	require.Equal(t, types.QValueGeometry{}, withGeoSRID(types.QValueGeometry{}, 3857), "empty fallback value stays empty")
	require.Equal(t, types.QValueNull(types.QValueKindGeometry), withGeoSRID(types.QValueNull(types.QValueKindGeometry), 3857))
}

// A self-intersecting polygon is invalid to PostGIS but stored by it, and the changefeed sends it as it is. For
// Postgres destinations at InternalVersion_PgConstrainedGeo the snapshot keeps it too, instead of writing NULL.
func TestInvalidGeoKeptForPostgres(t *testing.T) {
	const bowtie = "POLYGON((0 0,1 1,1 0,0 1,0 0))"
	wkb, err := datatypes.GeoToWKB(bowtie)
	require.NoError(t, err)
	field := types.QField{Name: "g", Type: types.QValueKindGeometry, Nullable: true}
	snapshot := func(version uint32, dst protos.DBType) types.QValue {
		qv, err := qvalueFromCrdbValue(field, dst, 0, pgtype.NewMap(), hex.EncodeToString(wkb), newPgValues(version, dst))
		require.NoError(t, err)
		return qv
	}
	require.Equal(t, types.QValueGeometry{Val: "POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))"}, snapshot(shared.InternalVersion_Latest, protos.DBType_POSTGRES))
	cdc, err := qvalueFromChangefeedJSON(field,
		json.RawMessage(`{"type":"Polygon","coordinates":[[[0,0],[1,1],[1,0],[0,1],[0,0]]]}`),
		newPgValues(shared.InternalVersion_Latest, protos.DBType_POSTGRES))
	require.NoError(t, err)
	require.Equal(t, cdc, snapshot(shared.InternalVersion_Latest, protos.DBType_POSTGRES), "same on both paths")
	// older mirrors and other destinations keep validating
	require.Equal(t, types.QValueNull(types.QValueKindGeometry),
		snapshot(shared.InternalVersion_PgConstrainedGeo-1, protos.DBType_POSTGRES))
	require.Equal(t, types.QValueNull(types.QValueKindGeometry),
		snapshot(shared.InternalVersion_Latest, protos.DBType_CLICKHOUSE))
}
