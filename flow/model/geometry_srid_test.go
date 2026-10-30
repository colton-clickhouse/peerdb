package model

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	geom "github.com/twpayne/go-geos"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// the COPY path writes EWKB with the SRID beside the WKT (MySQL) or in an EWKT prefix, and plain WKB without keepSRID
func TestGeoWktToWkbSRID(t *testing.T) {
	for _, tc := range []struct {
		wkt      string
		srid     uint32
		keepSRID bool
		want     int
	}{
		{"POINT(1 2)", 0, true, 0},
		{"POINT(1 2)", 4326, true, 4326},
		{"SRID=3857;POINT(1 2)", 0, true, 3857},
		{"SRID=3857;POINT(1 2)", 4326, true, 3857},
		{"POINT(1 2)", 4326, false, 0},
		{"SRID=3857;POINT(1 2)", 0, false, 0},
	} {
		wkb, err := geoWktToWkb(tc.wkt, tc.srid, tc.keepSRID)
		require.NoError(t, err)
		g, err := geom.NewGeomFromWKB(wkb)
		require.NoError(t, err, hex.EncodeToString(wkb))
		require.Equal(t, tc.want, g.SRID(), "%s srid=%d keep=%v", tc.wkt, tc.srid, tc.keepSRID)
		require.Equal(t, "POINT (1 2)", g.ToWKT())
	}
}

func TestRecordItemsGeometryAsEWKT(t *testing.T) {
	items := NewRecordItems(2)
	items.AddColumn("g", types.QValueGeometry{Val: "POINT(1 2)", SRID: 4326})
	items.AddColumn("z", types.QValueGeometry{Val: "POINT(1 2)"})

	j, err := items.ToJSONWithOptions(ToJSONOptions{GeometryAsEWKT: true})
	require.NoError(t, err)
	require.JSONEq(t, `{"g":"SRID=4326;POINT(1 2)","z":"POINT(1 2)"}`, j)

	j, err = items.ToJSONWithOptions(ToJSONOptions{})
	require.NoError(t, err)
	require.JSONEq(t, `{"g":"POINT(1 2)","z":"POINT(1 2)"}`, j)
}

// MySQL SRIDs are unsigned 32-bit; PostGIS stores at most 999999 and would change a larger one, so it's refused
func TestGeoSRIDAbovePostGISMaximum(t *testing.T) {
	_, err := geoWktToWkb("POINT(1 2)", 1<<31, true)
	require.ErrorContains(t, err, "SRID 2147483648")
	_, err = geoWktToWkb("POINT(1 2)", MaxPostGISSRID+1, true)
	require.Error(t, err)
	_, err = geoWktToWkb("POINT(1 2)", 1<<31, false)
	require.NoError(t, err, "older mirrors write no SRID")
	items := NewRecordItems(1)
	items.AddColumn("g", types.QValueGeometry{Val: "POINT(1 2)", SRID: 1 << 31})
	_, err = items.ToJSONWithOptions(ToJSONOptions{GeometryAsEWKT: true})
	require.ErrorContains(t, err, "column g")
}
