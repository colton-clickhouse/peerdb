package model

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	geom "github.com/twpayne/go-geos"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// the COPY path writes EWKB with the SRID of an EWKT prefix, else the one beside the WKT (MySQL), and plain WKB
// without GeoSRIDs
func TestGeoWkbSRID(t *testing.T) {
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
		src := NewQRecordCopyFromSource(nil, PgCopyOptions{GeoSRIDs: tc.keepSRID})
		wkb, err := src.geoWkb(tc.wkt, tc.srid)
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
	keep := NewQRecordCopyFromSource(nil, PgCopyOptions{GeoSRIDs: true})
	_, err := keep.geoWkb("POINT(1 2)", 1<<31)
	require.ErrorContains(t, err, "SRID 2147483648")
	require.ErrorAs(t, err, new(*exceptions.PostGISSRIDOutOfRangeError), "classified as an unsupported value")
	_, err = keep.geoWkb("POINT(1 2)", MaxPostGISSRID+1)
	require.Error(t, err)
	_, err = keep.geoWkb("SRID=-1;POINT(1 2)", 0)
	require.ErrorContains(t, err, "invalid SRID", "a malformed or negative prefix")
	_, err = NewQRecordCopyFromSource(nil, PgCopyOptions{}).geoWkb("POINT(1 2)", 1<<31)
	require.NoError(t, err, "older mirrors write no SRID")
	items := NewRecordItems(1)
	items.AddColumn("g", types.QValueGeometry{Val: "POINT(1 2)", SRID: 1 << 31})
	_, err = items.ToJSONWithOptions(ToJSONOptions{GeometryAsEWKT: true})
	require.ErrorContains(t, err, "column g")
	require.ErrorAs(t, err, new(*exceptions.PostGISSRIDOutOfRangeError))
}
