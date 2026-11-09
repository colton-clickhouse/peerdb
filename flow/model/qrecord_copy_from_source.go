package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	geo "github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func constructArray[T any](v []T) *pgtype.Array[T] {
	return &pgtype.Array[T]{
		Elements: v,
		Dims:     []pgtype.ArrayDimension{{Length: int32(len(v)), LowerBound: 1}},
		Valid:    true,
	}
}

// pgDuration is a TIME value for COPY. MySQL TIME is a duration (-838:59:59 to 838:59:59), and its target
// column can be time (PeerDB's mapping) or interval (pre-created). pgx encodes pgtype.Time into interval
// through its text form, which puts the sign on each field (-838:-59:-59, read back as -837:00:01);
// as an IntervalValuer it goes in as microseconds, like it does into time.
type pgDuration struct{ pgtype.Time }

func (d pgDuration) IntervalValue() (pgtype.Interval, error) {
	return pgtype.Interval{Microseconds: d.Microseconds, Valid: d.Valid}, nil
}

// PgCopyOptions are the internal-version dependent conversions of QRecordCopyFromSource.
type PgCopyOptions struct {
	// GeoSRIDs keeps geometry / geography SRIDs (EWKB) instead of dropping them (InternalVersion_PgGeoKeepSRID)
	GeoSRIDs bool
}

type QRecordCopyFromSource struct {
	stream        *QRecordStream
	currentRecord []types.QValue
	pgGeoSRIDs    bool
}

func NewQRecordCopyFromSource(
	stream *QRecordStream,
	opts PgCopyOptions,
) *QRecordCopyFromSource {
	return &QRecordCopyFromSource{
		stream:        stream,
		currentRecord: nil,
		pgGeoSRIDs:    opts.GeoSRIDs,
	}
}

func (src *QRecordCopyFromSource) Next() bool {
	rec, ok := <-src.stream.Records
	src.currentRecord = rec
	return ok || src.Err() != nil
}

func geoWktToWkb(wkt string) ([]byte, error) {
	if strings.HasPrefix(wkt, "SRID=") {
		_, wktWithoutSRID, found := strings.Cut(wkt, ";")
		if found {
			wkt = wktWithoutSRID
		}
	}

	return geo.GeoToWKB(wkt)
}

// geoWkb is the binary COPY value of a geometry / geography WKT. With pgGeoSRIDs it's EWKB with the SRID of an
// "SRID=n;" prefix, else fallbackSRID (MySQL keeps the SRID beside plain WKT; 0 for none), refused above PostGIS's
// maximum. Without it, WKB without the SRID, as older mirrors write.
func (src *QRecordCopyFromSource) geoWkb(wkt string, fallbackSRID uint32) ([]byte, error) {
	if !src.pgGeoSRIDs {
		return geoWktToWkb(wkt)
	}
	srid := fallbackSRID
	if rest, ok := strings.CutPrefix(wkt, "SRID="); ok {
		sridText, wktWithoutSRID, found := strings.Cut(rest, ";")
		if found {
			n, err := strconv.ParseUint(sridText, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid SRID in %q: %w", wkt, err)
			}
			srid, wkt = uint32(n), wktWithoutSRID
		}
	}
	if err := CheckPostGISSRID(srid); err != nil {
		return nil, err
	}
	return geo.GeoToEWKB(wkt, int(srid))
}

func (src *QRecordCopyFromSource) Values() ([]any, error) {
	if err := src.Err(); err != nil {
		return nil, err
	}

	values := make([]any, len(src.currentRecord))
	for i, qValue := range src.currentRecord {
		if qValue == nil || qValue.Value() == nil {
			values[i] = nil
			continue
		}

		switch v := qValue.(type) {
		case types.QValueFloat32:
			values[i] = v.Val
		case types.QValueFloat64:
			values[i] = v.Val
		case types.QValueInt8:
			values[i] = v.Val
		case types.QValueInt16:
			values[i] = v.Val
		case types.QValueInt32:
			values[i] = v.Val
		case types.QValueInt64:
			values[i] = v.Val
		case types.QValueUInt8:
			values[i] = v.Val
		case types.QValueUInt16:
			values[i] = v.Val
		case types.QValueUInt32:
			values[i] = v.Val
		case types.QValueUInt64:
			values[i] = v.Val
		case types.QValueBoolean:
			values[i] = v.Val
		case types.QValueQChar:
			values[i] = rune(v.Val)
		case types.QValueString:
			values[i] = v.Val
		case types.QValueEnum:
			values[i] = v.Val
		case types.QValueUint16Enum:
			values[i] = v.Val
		case types.QValueUint64Set:
			values[i] = v.Val
		case types.QValueCIDR:
			values[i] = v.Val
		case types.QValueINET:
			values[i] = v.Val
		case types.QValueMacaddr:
			values[i] = v.Val
		case types.QValueTime:
			values[i] = pgDuration{pgtype.Time{Microseconds: int64(v.Val / time.Microsecond), Valid: true}}
		case types.QValueTimeTZ:
			values[i] = pgtype.Time{Microseconds: int64(v.Val / time.Microsecond), Valid: true}
		case types.QValueInterval:
			values[i] = v.Val
		case types.QValueTimestamp:
			values[i] = pgtype.Timestamp{Time: v.Val, Valid: true}
		case types.QValueTimestampTZ:
			values[i] = pgtype.Timestamptz{Time: v.Val, Valid: true}
		case types.QValueUUID:
			values[i] = v.Val
		case types.QValueNumeric:
			values[i] = v.Val
		case types.QValueBytes:
			// a NULL arrives as QValueNull, so a nil Val is an empty value: pgx would write nil as NULL
			if v.Val == nil {
				values[i] = []byte{}
			} else {
				values[i] = v.Val
			}
		case types.QValueDate:
			values[i] = pgtype.Date{Time: v.Val, Valid: true}
		case types.QValueHStore:
			values[i] = v.Val
		case types.QValueGeography:
			wkb, err := src.geoWkb(v.Val, 0)
			if err != nil {
				return nil, err
			}
			values[i] = wkb
		case types.QValueGeometry:
			wkb, err := src.geoWkb(v.Val, v.SRID)
			if err != nil {
				return nil, err
			}
			values[i] = wkb
		case types.QValuePoint:
			wkb, err := geoWktToWkb(v.Val)
			if err != nil {
				return nil, err
			}
			values[i] = wkb
		case types.QValueArrayString:
			values[i] = constructArray(v.Val)
		case types.QValueArrayEnum:
			values[i] = constructArray(v.Val)
		case types.QValueArrayDate:
			values[i] = constructArray(v.Val)
		case types.QValueArrayInterval:
			values[i] = constructArray(v.Val)
		case types.QValueArrayTimestamp:
			values[i] = constructArray(v.Val)
		case types.QValueArrayTimestampTZ:
			values[i] = constructArray(v.Val)
		case types.QValueArrayInt16:
			values[i] = constructArray(v.Val)
		case types.QValueArrayInt32:
			values[i] = constructArray(v.Val)
		case types.QValueArrayInt64:
			values[i] = constructArray(v.Val)
		case types.QValueArrayFloat32:
			values[i] = constructArray(v.Val)
		case types.QValueArrayFloat64:
			values[i] = constructArray(v.Val)
		case types.QValueArrayBoolean:
			values[i] = constructArray(v.Val)
		case types.QValueArrayUUID:
			values[i] = constructArray(v.Val)
		case types.QValueArrayNumeric:
			values[i] = constructArray(v.Val)
		case types.QValueJSON:
			values[i] = v.Val
		case types.QValueArrayJSON:
			var arrayJ []any
			if err := json.Unmarshal([]byte(v.Val), &arrayJ); err != nil {
				return nil, fmt.Errorf("failed to unmarshal JSON array: %v", err)
			}
			values[i] = arrayJ
		// And so on for the other types...
		default:
			return nil, fmt.Errorf("unsupported value type %T", qValue)
		}
	}
	return values, nil
}

func (src *QRecordCopyFromSource) Err() error {
	return src.stream.Err()
}

// MaxPostGISSRID is the largest SRID PostGIS stores; it remaps larger ones (and a geometry SRID above it can't be
// used), so they're refused rather than replicated as another SRID
const MaxPostGISSRID = 999999

func CheckPostGISSRID(srid uint32) error {
	if srid > MaxPostGISSRID {
		return fmt.Errorf("SRID %d is above the largest SRID PostGIS stores (%d), which would change it", srid, MaxPostGISSRID)
	}
	return nil
}
