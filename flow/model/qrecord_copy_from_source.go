package model

import (
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

// timetzBinary is PG's binary timetz (int64 microseconds, int32 zone in seconds west of UTC) for a UTC
// time of day. pgx has no timetz codec: a pgtype.Time went out as an 8-byte time, which the server rejects
// for a timetz column (08P01 insufficient data left in message). Unknown OIDs take []byte as-is.
func timetzBinary(d time.Duration) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b, uint64(d/time.Microsecond))
	return b
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
	// pgIntervals writes intervals as intervals, and as their Postgres text into text columns (InternalVersion_PgInterval)
	pgIntervals bool
}

func NewQRecordCopyFromSource(
	stream *QRecordStream,
	pgIntervals bool,
) *QRecordCopyFromSource {
	return &QRecordCopyFromSource{
		stream:        stream,
		currentRecord: nil,
		pgIntervals:   pgIntervals,
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
			values[i] = timetzBinary(v.Val)
		case types.QValueInterval:
			if src.pgIntervals {
				iv, err := newPgInterval(v.Val)
				if err != nil {
					return nil, err
				}
				values[i] = iv
			} else {
				values[i] = v.Val
			}
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
			if src.pgIntervals {
				arr, err := pgIntervalArray(v.Val, v.Nulls)
				if err != nil {
					return nil, err
				}
				values[i] = arr
			} else {
				values[i] = constructNullableArray(v.Val, v.Nulls)
			}
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
