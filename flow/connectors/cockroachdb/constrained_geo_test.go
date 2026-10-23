package conncockroachdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// #14: information_schema.columns.data_type of constrained columns, as CockroachDB 25.4 reports them.
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
