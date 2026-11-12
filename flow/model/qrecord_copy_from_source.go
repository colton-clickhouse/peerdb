package model

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	geo "github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// timetzMicros is a UTC time of day in microseconds. Converting a time with a zone to UTC can leave the day
// (01:00+05 is -4h, 23:30-05 is 28:30), and the server only takes 0 to 24:00, so it's wrapped into the day: the same
// instant of day in UTC.
func timetzMicros(d time.Duration) int64 {
	const day = int64(24 * time.Hour / time.Microsecond)
	us := int64(d / time.Microsecond)
	if us < 0 || us > day {
		us = (us%day + day) % day
	}
	return us
}

// timetzBinary is PG's binary timetz (int64 microseconds, int32 zone in seconds west of UTC) for a UTC time of day.
func timetzBinary(d time.Duration) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b, uint64(timetzMicros(d)))
	return b
}

// pgTimeTZ is a timetz for COPY. pgx picks the encoding by the target column. pgx has no timetz codec: a
// pgtype.Time went out as an 8-byte time, which the server rejects for a timetz column (08P01 insufficient data left
// in message), so a timetz column, whose OID pgx doesn't know, gets PG's binary timetz through Value. A pre-created
// time column gets the UTC time of day, and a text column its text, as CDC writes them. A bytea column is refused, as
// normalize refuses it: bytea takes binary source columns only.
type pgTimeTZ struct {
	d time.Duration
}

func (v pgTimeTZ) Value() (driver.Value, error) {
	return timetzBinary(v.d), nil
}

func (v pgTimeTZ) TimeValue() (pgtype.Time, error) {
	return pgtype.Time{Microseconds: timetzMicros(v.d), Valid: true}, nil
}

// TextValue is the timetz as Postgres prints it, in UTC ("04:30:00+00"), as normalize's cast through timetz writes
// it into a text column
func (v pgTimeTZ) TextValue() (pgtype.Text, error) {
	us := timetzMicros(v.d)
	s := fmt.Sprintf("%02d:%02d:%02d", us/3_600_000_000, us/60_000_000%60, us/1_000_000%60)
	if frac := us % 1_000_000; frac != 0 {
		s += strings.TrimRight(fmt.Sprintf(".%06d", frac), "0")
	}
	return pgtype.Text{String: s + "+00", Valid: true}, nil
}

// BytesValue refuses a bytea column, which would otherwise take the 12 bytes of Value: bytea takes binary source
// columns only, and normalize fails the same value in CDC
func (v pgTimeTZ) BytesValue() ([]byte, error) {
	return nil, errors.New("a TIMETZ value can't go into a bytea column: bytea takes binary source columns only")
}

// StripNulBytes removes U+0000, which PG text can't hold (22021 in COPY, and JSONB refuses \u0000). Opt-in
// (PEERDB_PG_STRIP_NUL_BYTES): the value written to PG then differs from the source.
func StripNulBytes(s string) string {
	if strings.IndexByte(s, 0) < 0 {
		return s
	}
	return strings.ReplaceAll(s, "\x00", "")
}

func stripNulBytesAll(a []string) []string {
	if !slices.ContainsFunc(a, func(s string) bool { return strings.IndexByte(s, 0) >= 0 }) {
		return a
	}
	out := make([]string, len(a))
	for i, s := range a {
		out[i] = StripNulBytes(s)
	}
	return out
}

func (src *QRecordCopyFromSource) nulBytes(s string) string {
	if src.stripNulBytes {
		return StripNulBytes(s)
	}
	return s
}

func (src *QRecordCopyFromSource) nulBytesAll(a []string) []string {
	if src.stripNulBytes {
		return stripNulBytesAll(a)
	}
	return a
}

func constructArray[T any](v []T) *pgtype.Array[T] {
	return &pgtype.Array[T]{
		Elements: v,
		Dims:     []pgtype.ArrayDimension{{Length: int32(len(v)), LowerBound: 1}},
		Valid:    true,
	}
}

// constructNullableArray is constructArray with NULL elements where nulls says so (types.ArrayNulls).
func constructNullableArray[T any](v []T, nulls []bool) any {
	if nulls == nil {
		return constructArray(v)
	}
	elems := make([]*T, len(v))
	for i := range v {
		if !nulls[i] {
			elems[i] = &v[i]
		}
	}
	return constructArray(elems)
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
	// stripNulBytes removes U+0000 from strings (WithStripNulBytes)
	stripNulBytes bool
}

// WithStripNulBytes makes src remove NUL bytes from strings, which Postgres can't store (PEERDB_PG_STRIP_NUL_BYTES).
func (src *QRecordCopyFromSource) WithStripNulBytes() *QRecordCopyFromSource {
	src.stripNulBytes = true
	return src
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
		if err := types.CheckArrayNulls(qValue); err != nil {
			return nil, err
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
			values[i] = src.nulBytes(v.Val)
		case types.QValueEnum:
			values[i] = src.nulBytes(v.Val)
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
			values[i] = pgTimeTZ{v.Val}
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
			values[i] = v.Val
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
			values[i] = constructNullableArray(src.nulBytesAll(v.Val), v.Nulls)
		case types.QValueArrayEnum:
			values[i] = constructNullableArray(src.nulBytesAll(v.Val), v.Nulls)
		case types.QValueArrayDate:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayInterval:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayTimestamp:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayTimestampTZ:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayInt16:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayInt32:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayInt64:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayFloat32:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayFloat64:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayBoolean:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayUUID:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayNumeric:
			values[i] = constructNullableArray(v.Val, v.Nulls)
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
		return exceptions.NewPostGISSRIDOutOfRangeError(srid, MaxPostGISSRID)
	}
	return nil
}
