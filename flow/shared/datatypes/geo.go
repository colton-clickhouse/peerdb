package datatypes

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	geom "github.com/twpayne/go-geos"
)

// GeoToWKT is GeoValidate without the validity check, for destinations that store invalid shapes (PostGIS does).
func GeoToWKT(hexWkb string) (string, error) {
	wkb, err := hex.DecodeString(hexWkb)
	if err != nil {
		return "", fmt.Errorf("ignoring invalid WKB: %s %w", wkb, err)
	}
	geometryObject, err := geom.NewGeomFromWKB(wkb)
	if err != nil {
		return "", err
	}
	wkt := geometryObject.ToWKT()
	if srid := geometryObject.SRID(); srid != 0 {
		wkt = fmt.Sprintf("SRID=%d;%s", srid, wkt)
	}
	return wkt, nil
}

// GeoValidate returns the WKT representation of the geometry object if it is valid
func GeoValidate(hexWkb string) (string, error) {
	// Decode the WKB hex string into binary
	wkb, hexErr := hex.DecodeString(hexWkb)
	if hexErr != nil {
		return "", fmt.Errorf("ignoring invalid WKB: %s %w", wkb, hexErr)
	}

	// UnmarshalWKB performs geometry validation along with WKB parsing
	geometryObject, geoErr := geom.NewGeomFromWKB(wkb)
	if geoErr != nil {
		return "", geoErr
	}

	invalidReason := geometryObject.IsValidReason()
	if invalidReason != "Valid Geometry" {
		return "", fmt.Errorf("ignoring invalid geometry shape %s: %s", hexWkb, invalidReason)
	}

	wkt := geometryObject.ToWKT()

	if SRID := geometryObject.SRID(); SRID != 0 {
		wkt = fmt.Sprintf("SRID=%d;%s", geometryObject.SRID(), wkt)
	}
	return wkt, nil
}

func GeoToWKB(wkt string) ([]byte, error) {
	// UnmarshalWKB performs geometry validation along with WKB parsing
	geometryObject, geoErr := geom.NewGeomFromWKT(wkt)
	if geoErr != nil {
		return []byte{}, geoErr
	}

	return geometryObject.ToWKB(), nil
}

// geoShapes are PostGIS's geometry type codes, the shape field of a geometry / geography typmod.
var geoShapes = [...]string{
	"Geometry", "Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection",
}

// maxGeoSRID is the largest SRID a PostGIS typmod holds.
const maxGeoSRID = 999999

// MakeGeoTypmod encodes a constrained geometry / geography type's arguments, as CockroachDB and PostGIS write them
// ("point,4326", "PointZ", "geometry,3857"), as a PostGIS typmod (the layout of Postgres's atttypmod for these
// columns): SRID << 8 | shape << 2 | Z << 1 | M. ok is false for arguments it doesn't recognize.
func MakeGeoTypmod(args string) (int32, bool) {
	shapeText, sridText, hasSRID := strings.Cut(args, ",")
	shapeText = strings.ToLower(strings.TrimSpace(shapeText))
	typmod := int32(-1)
	for i, shape := range geoShapes {
		rest, ok := strings.CutPrefix(shapeText, strings.ToLower(shape))
		if !ok {
			continue
		}
		var dims int32
		switch rest {
		case "":
		case "z":
			dims = 2
		case "m":
			dims = 1
		case "zm":
			dims = 3
		default:
			continue
		}
		typmod = int32(i)<<2 | dims
		break
	}
	if typmod == -1 {
		return 0, false
	}
	if hasSRID {
		srid, err := strconv.Atoi(strings.TrimSpace(sridText))
		if err != nil || srid < 0 || srid > maxGeoSRID {
			return 0, false
		}
		typmod |= int32(srid) << 8
	}
	return typmod, true
}

// GeoTypmodSRID is the SRID of a PostGIS typmod; 0 for none.
func GeoTypmodSRID(typmod int32) int {
	if typmod < 0 {
		return 0
	}
	return int(typmod>>8) & 0x1FFFFF
}

// FormatGeoType is the Postgres type for a PostGIS typmod: FormatGeoType("geometry", typmod) gives
// "geometry(PointZ,3857)", or "geometry(Polygon)" without an SRID.
func FormatGeoType(base string, typmod int32) string {
	shape := geoShapes[0]
	if i := int(typmod>>2) & 0x3F; i < len(geoShapes) {
		shape = geoShapes[i]
	}
	switch typmod & 3 {
	case 3:
		shape += "ZM"
	case 2:
		shape += "Z"
	case 1:
		shape += "M"
	}
	if srid := GeoTypmodSRID(typmod); srid != 0 {
		return fmt.Sprintf("%s(%s,%d)", base, shape, srid)
	}
	return fmt.Sprintf("%s(%s)", base, shape)
}

// GeoToEWKB is GeoToWKB with the SRID in the output (EWKB) when it isn't 0.
func GeoToEWKB(wkt string, srid int) ([]byte, error) {
	geometryObject, geoErr := geom.NewGeomFromWKT(wkt)
	if geoErr != nil {
		return []byte{}, geoErr
	}
	if srid == 0 {
		return geometryObject.ToWKB(), nil
	}
	return geometryObject.SetSRID(srid).ToEWKBWithSRID(), nil
}
