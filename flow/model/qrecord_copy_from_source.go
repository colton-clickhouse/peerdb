package model

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"

	geo "github.com/PeerDB-io/peerdb/flow/shared/datatypes"
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
// time column gets the UTC time of day, and a text (or bytea) column its text, as CDC writes them.
type pgTimeTZ struct {
	d time.Duration
}

func (v pgTimeTZ) Value() (driver.Value, error) {
	return timetzBinary(v.d), nil
}

func (v pgTimeTZ) TimeValue() (pgtype.Time, error) {
	return pgtype.Time{Microseconds: timetzMicros(v.d), Valid: true}, nil
}

func (v pgTimeTZ) TextValue() (pgtype.Text, error) {
	return pgtype.Text{String: types.FormatExtendedTimeDuration(time.Duration(timetzMicros(v.d)) * time.Microsecond),
		Valid: true}, nil
}

func (v pgTimeTZ) BytesValue() ([]byte, error) {
	t, err := v.TextValue()
	return []byte(t.String), err
}

// StripNulBytes removes U+0000, which PG text can't hold (22021 in COPY, and JSONB refuses \u0000).
// The value written to PG then differs from the source: a deliberate behavioral difference.
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

// constructSpecialArray is constructNullableArray for arrays with special elements (types.SpecialValue): conv builds
// each element's pgtype value from its Val and special value.
func constructSpecialArray[T any, P any](
	v []T, nulls []bool, specials []types.SpecialValue, conv func(T, types.SpecialValue) P,
) *pgtype.Array[*P] {
	elems := make([]*P, len(v))
	for i := range v {
		if nulls == nil || !nulls[i] {
			el := conv(v[i], specials[i])
			elems[i] = &el
		}
	}
	return constructArray(elems)
}

func pgInfinity(sv types.SpecialValue) pgtype.InfinityModifier {
	switch sv {
	case types.SpecialInfinity:
		return pgtype.Infinity
	case types.SpecialNegInfinity:
		return pgtype.NegativeInfinity
	}
	return pgtype.Finite
}

func pgSpecialNumeric(sv types.SpecialValue) pgtype.Numeric {
	return pgtype.Numeric{NaN: sv == types.SpecialNaN, InfinityModifier: pgInfinity(sv), Valid: true}
}

type QRecordCopyFromSource struct {
	stream        *QRecordStream
	currentRecord []types.QValue
	// noNumericInfinity writes numeric +-Infinity as NULL (WithoutNumericInfinity), for Postgres before 14
	noNumericInfinity bool
}

// WithoutNumericInfinity makes src write numeric +-Infinity as NULL, for destinations before Postgres 14.
func (src *QRecordCopyFromSource) WithoutNumericInfinity() *QRecordCopyFromSource {
	src.noNumericInfinity = true
	return src
}

func NewQRecordCopyFromSource(
	stream *QRecordStream,
) *QRecordCopyFromSource {
	return &QRecordCopyFromSource{
		stream:        stream,
		currentRecord: nil,
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

func (src *QRecordCopyFromSource) Values() ([]any, error) {
	if err := src.Err(); err != nil {
		return nil, err
	}

	values := make([]any, len(src.currentRecord))
	for i, qValue := range src.currentRecord {
		if src.noNumericInfinity {
			qValue = WithoutNumericInfinity(qValue)
		}
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
			values[i] = StripNulBytes(v.Val)
		case types.QValueEnum:
			values[i] = StripNulBytes(v.Val)
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
			values[i] = pgtype.Time{Microseconds: int64(v.Val / time.Microsecond), Valid: true}
		case types.QValueTimeTZ:
			values[i] = pgTimeTZ{v.Val}
		case types.QValueInterval:
			values[i] = v.Val
		case types.QValueTimestamp:
			values[i] = pgtype.Timestamp{Time: v.Val, InfinityModifier: pgInfinity(v.Special), Valid: true}
		case types.QValueTimestampTZ:
			values[i] = pgtype.Timestamptz{Time: v.Val, InfinityModifier: pgInfinity(v.Special), Valid: true}
		case types.QValueUUID:
			values[i] = v.Val
		case types.QValueNumeric:
			if v.Special != types.SpecialNone {
				values[i] = pgSpecialNumeric(v.Special)
			} else {
				values[i] = v.Val
			}
		case types.QValueBytes:
			values[i] = v.Val
		case types.QValueDate:
			values[i] = pgtype.Date{Time: v.Val, InfinityModifier: pgInfinity(v.Special), Valid: true}
		case types.QValueHStore:
			values[i] = v.Val
		case types.QValueGeography:
			wkb, err := geoWktToWkb(v.Val)
			if err != nil {
				return nil, err
			}
			values[i] = wkb
		case types.QValueGeometry:
			wkb, err := geoWktToWkb(v.Val)
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
			values[i] = constructNullableArray(stripNulBytesAll(v.Val), v.Nulls)
		case types.QValueArrayEnum:
			values[i] = constructNullableArray(stripNulBytesAll(v.Val), v.Nulls)
		case types.QValueArrayDate:
			if v.Specials != nil {
				values[i] = constructSpecialArray(v.Val, v.Nulls, v.Specials, func(t time.Time, sv types.SpecialValue) pgtype.Date {
					return pgtype.Date{Time: t, InfinityModifier: pgInfinity(sv), Valid: true}
				})
			} else {
				values[i] = constructNullableArray(v.Val, v.Nulls)
			}
		case types.QValueArrayInterval:
			values[i] = constructNullableArray(v.Val, v.Nulls)
		case types.QValueArrayTimestamp:
			if v.Specials != nil {
				values[i] = constructSpecialArray(v.Val, v.Nulls, v.Specials, func(t time.Time, sv types.SpecialValue) pgtype.Timestamp {
					return pgtype.Timestamp{Time: t, InfinityModifier: pgInfinity(sv), Valid: true}
				})
			} else {
				values[i] = constructNullableArray(v.Val, v.Nulls)
			}
		case types.QValueArrayTimestampTZ:
			if v.Specials != nil {
				values[i] = constructSpecialArray(v.Val, v.Nulls, v.Specials, func(t time.Time, sv types.SpecialValue) pgtype.Timestamptz {
					return pgtype.Timestamptz{Time: t, InfinityModifier: pgInfinity(sv), Valid: true}
				})
			} else {
				values[i] = constructNullableArray(v.Val, v.Nulls)
			}
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
			if v.Specials != nil {
				values[i] = constructSpecialArray(v.Val, v.Nulls, v.Specials, func(d decimal.Decimal, sv types.SpecialValue) pgtype.Numeric {
					if sv != types.SpecialNone {
						return pgSpecialNumeric(sv)
					}
					return pgtype.Numeric{Int: d.Coefficient(), Exp: d.Exponent(), Valid: true}
				})
			} else {
				values[i] = constructNullableArray(v.Val, v.Nulls)
			}
		case types.QValueJSON:
			if v.IsArray {
				var arrayJ []any
				if err := json.Unmarshal([]byte(v.Val), &arrayJ); err != nil {
					return nil, fmt.Errorf("failed to unmarshal JSON array: %v", err)
				}

				values[i] = arrayJ
			} else {
				values[i] = v.Val
			}
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
