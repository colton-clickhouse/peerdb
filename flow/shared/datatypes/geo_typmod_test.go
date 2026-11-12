package datatypes

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// typmods as PostGIS 3 writes them (atttypmod of the same columns created on Postgres 18).
func TestGeoTypmod(t *testing.T) {
	for _, tc := range []struct {
		args   string
		typmod int32
		pgType string
	}{
		{"point,4326", 1107460, "geometry(Point,4326)"},
		{"Polygon", 12, "geometry(Polygon)"},
		{"pointz,3857", 987398, "geometry(PointZ,3857)"},
		{"geometry,3857", 987392, "geometry(Geometry,3857)"},
		{"multipolygonzm,4326", 1107483, "geometry(MultiPolygonZM,4326)"},
		{"PointM", 5, "geometry(PointM)"},
		{"GeometryCollection, 4326", 1107484, "geometry(GeometryCollection,4326)"},
	} {
		typmod, ok := MakeGeoTypmod(tc.args)
		require.True(t, ok, tc.args)
		require.Equal(t, tc.typmod, typmod, tc.args)
		require.Equal(t, tc.pgType, FormatGeoType("geometry", typmod), tc.args)
	}
	require.Equal(t, 4326, GeoTypmodSRID(1107460))
	require.Equal(t, 0, GeoTypmodSRID(12))
	require.Equal(t, 0, GeoTypmodSRID(-1))
	for _, bad := range []string{"", "circle", "pointx", "point,abc", "point,-1", "point,1000000"} {
		_, ok := MakeGeoTypmod(bad)
		require.False(t, ok, bad)
	}
}

func TestGeoToEWKB(t *testing.T) {
	b, err := GeoToEWKB("POINT(1 2)", 4326)
	require.NoError(t, err)
	require.Equal(t, "0101000020e6100000000000000000f03f0000000000000040", hex.EncodeToString(b))
	b, err = GeoToEWKB("POINT(1 2)", 0)
	require.NoError(t, err)
	require.Equal(t, "0101000000000000000000f03f0000000000000040", hex.EncodeToString(b))
}
