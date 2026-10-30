package model

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// The COPY keeps a geometry's SRID only with PgCopyOptions.GeoSRIDs (InternalVersion_PgConstrainedGeo): older mirrors
// write WKB without it, as before, and a value without one stays without one either way.
func TestCopyGeoSRIDs(t *testing.T) {
	const point = "0101000000000000000000f03f0000000000000040"             // POINT(1 2), plain WKB
	const point4326 = "0101000020e6100000000000000000f03f0000000000000040" // EWKB, SRID 4326
	for _, tc := range []struct {
		keep bool
		wkt  string
		want string
	}{
		{false, "SRID=4326;POINT(1 2)", point},
		{true, "SRID=4326;POINT(1 2)", point4326},
		{true, "POINT(1 2)", point},
		{false, "POINT(1 2)", point},
	} {
		src := &QRecordCopyFromSource{pgGeoSRIDs: tc.keep}
		wkb, err := src.geoWkb(tc.wkt)
		require.NoError(t, err)
		require.Equal(t, tc.want, hex.EncodeToString(wkb), "%s keep=%v", tc.wkt, tc.keep)
	}
}
