package conncockroachdb

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// crdbTypeToQValueKind maps CockroachDB information_schema column types to QValueKinds.
// data_type is information_schema.columns.data_type; udt_name disambiguates
// arrays (data_type 'ARRAY') and user-defined types (data_type 'USER-DEFINED').
func crdbTypeToQValueKind(dataType string, udtName string) types.QValueKind {
	switch strings.ToLower(dataType) {
	case "bigint", "int8":
		return types.QValueKindInt64
	case "oid":
		// the oid type is an unsigned 32-bit integer
		return types.QValueKindUInt32
	case "integer", "int4":
		return types.QValueKindInt32
	case "smallint", "int2":
		return types.QValueKindInt16
	case "numeric", "decimal":
		return types.QValueKindNumeric
	case "double precision", "float8":
		return types.QValueKindFloat64
	case "real", "float4":
		return types.QValueKindFloat32
	case "boolean", "bool":
		return types.QValueKindBoolean
	case "character varying", "character", "text", "name", `"char"`, "bit", "bit varying", "varbit":
		return types.QValueKindString
	case "bytes", "bytea", "blob":
		return types.QValueKindBytes
	case "uuid":
		return types.QValueKindUUID
	case "jsonb", "json":
		return types.QValueKindJSON
	case "timestamp without time zone", "timestamp":
		return types.QValueKindTimestamp
	case "timestamp with time zone", "timestamptz":
		return types.QValueKindTimestampTZ
	case "date":
		return types.QValueKindDate
	case "time without time zone", "time":
		return types.QValueKindTime
	case "time with time zone", "timetz":
		return types.QValueKindTimeTZ
	case "interval":
		return types.QValueKindInterval
	case "inet":
		return types.QValueKindINET
	case "geography":
		return types.QValueKindGeography
	case "geometry":
		return types.QValueKindGeometry
	case "array":
		return crdbArrayUdtToQValueKind(udtName)
	case "user-defined":
		// CockroachDB user-defined types are enums
		return types.QValueKindEnum
	default:
		// vector, tsvector, tsquery, refcursor and anything new: replicate as text
		return types.QValueKindString
	}
}

// crdbOIDToQValueKind maps wire type OIDs to QValueKinds for result columns
// that cannot be matched to a table column (e.g. custom query expressions).
func crdbOIDToQValueKind(oid uint32) types.QValueKind {
	switch oid {
	case pgtype.BoolOID:
		return types.QValueKindBoolean
	case pgtype.Int2OID:
		return types.QValueKindInt16
	case pgtype.Int4OID:
		return types.QValueKindInt32
	case pgtype.Int8OID:
		return types.QValueKindInt64
	case pgtype.OIDOID:
		return types.QValueKindUInt32
	case pgtype.Float4OID:
		return types.QValueKindFloat32
	case pgtype.Float8OID:
		return types.QValueKindFloat64
	case pgtype.NumericOID:
		return types.QValueKindNumeric
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID, pgtype.QCharOID:
		return types.QValueKindString
	case pgtype.ByteaOID:
		return types.QValueKindBytes
	case pgtype.UUIDOID:
		return types.QValueKindUUID
	case pgtype.JSONOID, pgtype.JSONBOID:
		return types.QValueKindJSON
	case pgtype.TimestampOID:
		return types.QValueKindTimestamp
	case pgtype.TimestamptzOID:
		return types.QValueKindTimestampTZ
	case pgtype.DateOID:
		return types.QValueKindDate
	case pgtype.TimeOID:
		return types.QValueKindTime
	case pgtype.TimetzOID:
		return types.QValueKindTimeTZ
	case pgtype.IntervalOID:
		return types.QValueKindInterval
	case pgtype.InetOID:
		return types.QValueKindINET
	case pgtype.Int2ArrayOID:
		return types.QValueKindArrayInt16
	case pgtype.Int4ArrayOID:
		return types.QValueKindArrayInt32
	case pgtype.Int8ArrayOID:
		return types.QValueKindArrayInt64
	case pgtype.Float4ArrayOID:
		return types.QValueKindArrayFloat32
	case pgtype.Float8ArrayOID:
		return types.QValueKindArrayFloat64
	case pgtype.NumericArrayOID:
		return types.QValueKindArrayNumeric
	case pgtype.BoolArrayOID:
		return types.QValueKindArrayBoolean
	case pgtype.UUIDArrayOID:
		return types.QValueKindArrayUUID
	case pgtype.JSONArrayOID, pgtype.JSONBArrayOID:
		return types.QValueKindArrayJSON
	case pgtype.DateArrayOID:
		return types.QValueKindArrayDate
	case pgtype.TimestampArrayOID:
		return types.QValueKindArrayTimestamp
	case pgtype.TimestamptzArrayOID:
		return types.QValueKindArrayTimestampTZ
	case pgtype.IntervalArrayOID:
		return types.QValueKindArrayInterval
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.BPCharArrayOID:
		return types.QValueKindArrayString
	default:
		return types.QValueKindString
	}
}

// pgValues are the conversions a Postgres destination gets from the mirror's internal version: values other
// destinations can't hold, so they keep the older fallbacks. The snapshot and changefeed paths share them.
type pgValues struct {
	// specialValues marks NaN and infinity values (InternalVersion_PgSpecialValues)
	specialValues bool
	// time2400 keeps TIME '24:00:00' instead of clamping it to 23:59:59.999999 (InternalVersion_PgTime2400)
	time2400 bool
	// invalidGeo keeps geometries PostGIS calls invalid (e.g. a self-intersecting polygon) instead of writing NULL
	// (InternalVersion_PgConstrainedGeo): Postgres stores them, and the changefeed sends them as they are
	invalidGeo bool
	// arrayInfinity: +-infinity date/timestamp array elements become defaultTime on destinations without
	// specialValues, on both paths, instead of the zero time (snapshot) or a parse error (changefeed)
	// (InternalVersion_ArrayConversionIntegrity)
	arrayInfinity bool
	defaultTime   time.Time
}

func newPgValues(version uint32, dstType protos.DBType) pgValues {
	pg := dstType == protos.DBType_POSTGRES
	return pgValues{
		specialValues: pg && version >= shared.InternalVersion_PgSpecialValues,
		time2400:      pg && version >= shared.InternalVersion_PgTime2400,
		invalidGeo:    pg && version >= shared.InternalVersion_PgConstrainedGeo,
		arrayInfinity: version >= shared.InternalVersion_ArrayConversionIntegrity,
		defaultTime:   qvalue.DefaultTime(dstType),
	}
}

// qvalueFromCrdbValue converts a value decoded by pgx into the QValue matching field.Type.
func qvalueFromCrdbValue(
	field types.QField,
	dstType protos.DBType,
	oid uint32,
	typeMap *pgtype.Map,
	value any,
	pg pgValues,
) (types.QValue, error) {
	if value == nil {
		return types.QValueNull(field.Type), nil
	}

	switch field.Type {
	case types.QValueKindBoolean:
		if v, ok := value.(bool); ok {
			return types.QValueBoolean{Val: v}, nil
		}
	case types.QValueKindInt16:
		if v, ok := value.(int16); ok {
			return types.QValueInt16{Val: v}, nil
		}
	case types.QValueKindInt32:
		if v, ok := value.(int32); ok {
			return types.QValueInt32{Val: v}, nil
		}
	case types.QValueKindInt64:
		if v, ok := value.(int64); ok {
			return types.QValueInt64{Val: v}, nil
		}
	case types.QValueKindUInt32:
		// pgx decodes the oid type as uint32
		if v, ok := value.(uint32); ok {
			return types.QValueUInt32{Val: v}, nil
		}
	case types.QValueKindFloat32:
		if v, ok := value.(float32); ok {
			return types.QValueFloat32{Val: v}, nil
		}
	case types.QValueKindFloat64:
		if v, ok := value.(float64); ok {
			return types.QValueFloat64{Val: v}, nil
		}
	case types.QValueKindNumeric:
		if v, ok := value.(pgtype.Numeric); ok {
			num, valid := numericToDecimal(v)
			if !valid {
				if sv := numericSpecial(v, field.Precision); pg.specialValues && sv != types.SpecialNone {
					return types.QValueNumeric{Precision: field.Precision, Scale: field.Scale, Special: sv}, nil
				}
				if field.Nullable {
					return types.QValueNull(types.QValueKindNumeric), nil
				}
				return types.QValueNumeric{Precision: field.Precision, Scale: field.Scale}, nil
			}
			return types.QValueNumeric{Val: num, Precision: field.Precision, Scale: field.Scale}, nil
		}
	case types.QValueKindString:
		return types.QValueString{Val: convertToString(typeMap, oid, value)}, nil
	case types.QValueKindEnum:
		return types.QValueEnum{Val: convertToString(typeMap, oid, value)}, nil
	case types.QValueKindBytes:
		if v, ok := value.([]byte); ok {
			return types.QValueBytes{Val: v}, nil
		}
	case types.QValueKindUUID:
		v, err := parseUUID(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse UUID: %w", err)
		}
		return types.QValueUUID{Val: v}, nil
	case types.QValueKindJSON:
		if v, ok := value.(string); ok {
			return types.QValueJSON{Val: v}, nil
		}
	case types.QValueKindTimestamp:
		switch v := value.(type) {
		case time.Time:
			return types.QValueTimestamp{Val: v}, nil
		case pgtype.InfinityModifier:
			if pg.specialValues {
				return types.QValueTimestamp{Special: infinitySpecial(v)}, nil
			}
			if field.Nullable {
				return types.QValueNull(field.Type), nil
			}
			return types.QValueTimestamp{Val: qvalue.DefaultTime(dstType)}, nil
		}
	case types.QValueKindTimestampTZ:
		switch v := value.(type) {
		case time.Time:
			return types.QValueTimestampTZ{Val: v}, nil
		case pgtype.InfinityModifier:
			if pg.specialValues {
				return types.QValueTimestampTZ{Special: infinitySpecial(v)}, nil
			}
			if field.Nullable {
				return types.QValueNull(field.Type), nil
			}
			return types.QValueTimestampTZ{Val: qvalue.DefaultTime(dstType)}, nil
		}
	case types.QValueKindDate:
		switch v := value.(type) {
		case time.Time:
			return types.QValueDate{Val: v}, nil
		case pgtype.InfinityModifier:
			if pg.specialValues {
				return types.QValueDate{Special: infinitySpecial(v)}, nil
			}
			if field.Nullable {
				return types.QValueNull(field.Type), nil
			}
			return types.QValueDate{Val: qvalue.DefaultTime(dstType)}, nil
		}
	case types.QValueKindTime:
		if v, ok := value.(pgtype.Time); ok {
			if !v.Valid {
				if field.Nullable {
					return types.QValueNull(types.QValueKindTime), nil
				}
				return types.QValueTime{}, nil
			}
			// CockroachDB permits TIME '24:00:00', which only Postgres can hold
			if !pg.time2400 {
				v.Microseconds = min(v.Microseconds, maxTimeMicroseconds)
			}
			return types.QValueTime{Val: time.Duration(v.Microseconds) * time.Microsecond}, nil
		}
	case types.QValueKindTimeTZ:
		if v, ok := value.(string); ok {
			d, err := parseTimeTZ(v)
			if err != nil {
				return nil, err
			}
			return types.QValueTimeTZ{Val: d}, nil
		}
	case types.QValueKindInterval:
		switch v := value.(type) {
		case string:
			// the snapshot reads intervals as text (buildSelectedColumns), parsed like the changefeed's
			str, err := parsePgIntervalToJSON(v)
			if err != nil {
				return nil, err
			}
			return types.QValueInterval{Val: str}, nil
		case pgtype.Interval:
			str, err := intervalToString(v)
			if err != nil {
				return nil, err
			}
			return types.QValueInterval{Val: str}, nil
		}
	case types.QValueKindINET:
		switch v := value.(type) {
		case string:
			return types.QValueINET{Val: normalizeInetText(v)}, nil
		case netip.Prefix:
			return types.QValueINET{Val: inetPrefixText(v)}, nil
		}
	case types.QValueKindGeography, types.QValueKindGeometry:
		wkbString, ok := value.(string)
		toWKT := datatypes.GeoValidate
		if pg.invalidGeo {
			toWKT = datatypes.GeoToWKT
		}
		wkt, err := toWKT(wkbString)
		if err != nil || !ok {
			if field.Nullable {
				return types.QValueNull(field.Type), nil
			} else if field.Type == types.QValueKindGeography {
				return types.QValueGeography{}, nil
			}
			return types.QValueGeometry{}, nil
		} else if field.Type == types.QValueKindGeography {
			return types.QValueGeography{Val: wkt}, nil
		}
		return types.QValueGeometry{Val: wkt}, nil
	case types.QValueKindArrayInt16:
		a, err := convertToArray[int16](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt16{Val: a}, nil
	case types.QValueKindArrayInt32:
		a, err := convertToArray[int32](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt32{Val: a}, nil
	case types.QValueKindArrayInt64:
		a, err := convertToArray[int64](field.Type, widenOIDElements(value))
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt64{Val: a}, nil
	case types.QValueKindArrayFloat32:
		a, err := convertToArray[float32](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayFloat32{Val: a}, nil
	case types.QValueKindArrayFloat64:
		a, err := convertToArray[float64](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayFloat64{Val: a}, nil
	case types.QValueKindArrayBoolean:
		a, err := convertToArray[bool](field.Type, value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayBoolean{Val: a}, nil
	case types.QValueKindArrayDate, types.QValueKindArrayTimestamp, types.QValueKindArrayTimestampTZ:
		var specials []types.SpecialValue
		if elems, ok := value.([]any); ok {
			// pgx decodes infinity elements as pgtype.InfinityModifier, which the element cast doesn't accept:
			// marked as specials for Postgres destinations, DefaultTime for others, the zero time on older mirrors
			replaced := slices.Clone(elems)
			if pg.specialValues {
				specials = make([]types.SpecialValue, len(elems))
			}
			for i, el := range elems {
				inf, ok := el.(pgtype.InfinityModifier)
				if !ok {
					continue
				}
				replaced[i] = time.Time{}
				if pg.specialValues {
					specials[i] = infinitySpecial(inf)
				} else if pg.arrayInfinity {
					replaced[i] = pg.defaultTime
				}
			}
			specials = types.CompactSpecials(specials)
			value = replaced
		}
		a, err := convertToArray[time.Time](field.Type, value)
		if err != nil {
			return nil, err
		}
		switch field.Type {
		case types.QValueKindArrayDate:
			return types.QValueArrayDate{Val: a, Specials: specials}, nil
		case types.QValueKindArrayTimestamp:
			return types.QValueArrayTimestamp{Val: a, Specials: specials}, nil
		default:
			return types.QValueArrayTimestampTZ{Val: a, Specials: specials}, nil
		}
	case types.QValueKindArrayUUID:
		a, err := parseUUIDArray(value)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayUUID{Val: a}, nil
	case types.QValueKindArrayNumeric:
		if v, ok := value.([]any); ok {
			numArr := make([]decimal.Decimal, 0, len(v))
			var specials []types.SpecialValue
			if pg.specialValues {
				specials = make([]types.SpecialValue, len(v))
			}
			for i, anyVal := range v {
				if anyVal == nil {
					numArr = append(numArr, decimal.Decimal{})
					continue
				}
				numVal, ok := anyVal.(pgtype.Numeric)
				if !ok {
					return nil, fmt.Errorf("failed to cast ArrayNumeric element: got %T", anyVal)
				}
				num, valid := numericToDecimal(numVal)
				if !valid {
					num = decimal.Decimal{}
					if pg.specialValues {
						specials[i] = numericSpecial(numVal, field.Precision)
					}
				}
				numArr = append(numArr, num)
			}
			return types.QValueArrayNumeric{
				Val: numArr, Precision: field.Precision, Scale: field.Scale, Specials: types.CompactSpecials(specials),
			}, nil
		}
	case types.QValueKindArrayInterval:
		if v, ok := value.([]any); ok {
			strs := make([]string, 0, len(v))
			for _, anyVal := range v {
				if anyVal == nil {
					strs = append(strs, "")
					continue
				}
				var str string
				var err error
				switch interval := anyVal.(type) {
				case string:
					str, err = parsePgIntervalToJSON(interval)
				case pgtype.Interval:
					str, err = intervalToString(interval)
				default:
					return nil, fmt.Errorf("failed to cast ArrayInterval element: got %T", anyVal)
				}
				if err != nil {
					return nil, err
				}
				strs = append(strs, str)
			}
			return types.QValueArrayInterval{Val: strs}, nil
		}
	case types.QValueKindArrayString:
		switch v := value.(type) {
		case string:
			return types.QValueArrayString{Val: shared.ParsePgArrayStringToStringSlice(v, ',')}, nil
		case []string:
			return types.QValueArrayString{Val: v}, nil
		case []any:
			strs := make([]string, 0, len(v))
			for _, anyVal := range v {
				if anyVal == nil {
					// NULL array element: empty string, matching the changefeed
					// path and ArrayCastElements' zero-value convention
					strs = append(strs, "")
					continue
				}
				if prefix, ok := anyVal.(netip.Prefix); ok {
					// INET[] elements, like scalar INET
					strs = append(strs, inetPrefixText(prefix))
					continue
				}
				if b, ok := anyVal.([]byte); ok {
					// BYTES[] elements in Postgres's bytea hex form, as the changefeed sends them
					strs = append(strs, `\x`+hex.EncodeToString(b))
					continue
				}
				strs = append(strs, convertToString(typeMap, arrayElementOID(typeMap, oid), anyVal))
			}
			return types.QValueArrayString{Val: strs}, nil
		}
	default:
		if v, ok := value.(string); ok {
			return types.QValueString{Val: v}, nil
		}
	}

	return nil, fmt.Errorf("failed to parse value %v (%T) into QValueKind %v", value, value, field.Type)
}

func convertToString(typeMap *pgtype.Map, oid uint32, value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	if tm, ok := value.(pgtype.Time); ok && tm.Valid {
		// pgx's text form always has 6 fractional digits; the server's (and the changefeed's) trims them
		return timeText(tm.Microseconds)
	}
	if oid != 0 {
		if buf, err := typeMap.Encode(oid, pgtype.TextFormatCode, value, nil); err == nil {
			return string(buf)
		}
	}
	if _, logged := fmtFallbackOIDs.LoadOrStore(oid, true); !logged {
		slog.Warn("[cockroachdb] no Postgres text form for a value, writing Go's formatting",
			slog.Uint64("oid", uint64(oid)), slog.String("goType", fmt.Sprintf("%T", value)))
	}
	return fmt.Sprint(value)
}

// timeText renders a time of day in microseconds as Postgres and CockroachDB do: HH:MM:SS, then the fraction
// without trailing zeros, if any (24:00:00 included).
func timeText(micros int64) string {
	s := fmt.Sprintf("%02d:%02d:%02d", micros/3_600_000_000, micros/60_000_000%60, micros/1_000_000%60)
	if frac := micros % 1_000_000; frac != 0 {
		s += "." + strings.TrimRight(fmt.Sprintf("%06d", frac), "0")
	}
	return s
}

// fmtFallbackOIDs are the OIDs convertToString has logged its fmt.Sprint fallback for (once per process each).
var fmtFallbackOIDs sync.Map

// arrayElementOID is the element type's OID of an array type, so elements render as Postgres text (TIME[] as
// 12:34:56, VARBIT[] as 101), or 0 if the type map doesn't know the array type.
func arrayElementOID(typeMap *pgtype.Map, arrayOID uint32) uint32 {
	if dt, ok := typeMap.TypeForOID(arrayOID); ok {
		if codec, ok := dt.Codec.(*pgtype.ArrayCodec); ok && codec.ElementType != nil {
			return codec.ElementType.OID
		}
	}
	return 0
}

// widenOIDElements turns OID[] elements, which pgx decodes as uint32, into the int64 the column's kind holds.
func widenOIDElements(value any) any {
	elems, ok := value.([]any)
	if !ok {
		return value
	}
	var widened []any
	for i, el := range elems {
		if u, ok := el.(uint32); ok {
			if widened == nil {
				widened = slices.Clone(elems)
			}
			widened[i] = int64(u)
		}
	}
	if widened == nil {
		return value
	}
	return widened
}

func convertToArray[T any](kind types.QValueKind, value any) ([]T, error) {
	switch v := value.(type) {
	case []T:
		return v, nil
	case []any:
		return shared.ArrayCastElements[T](v)
	}
	return nil, fmt.Errorf("failed to parse array %s from %T: %v", kind, value, value)
}

func numericToDecimal(numVal pgtype.Numeric) (decimal.Decimal, bool) {
	if !numVal.Valid || numVal.NaN ||
		numVal.InfinityModifier == pgtype.Infinity || numVal.InfinityModifier == pgtype.NegativeInfinity {
		return decimal.Decimal{}, false
	}
	return decimal.NewFromBigInt(numVal.Int, numVal.Exp), true
}

// numericSpecial is the special value of a NaN or infinite numeric, or SpecialNone. Postgres can't store +-Infinity
// in a numeric(p, s) column (CockroachDB can), so with a precision infinities keep the old fallback (NULL or 0).
func numericSpecial(numVal pgtype.Numeric, precision int16) types.SpecialValue {
	switch {
	case !numVal.Valid:
		return types.SpecialNone
	case numVal.NaN:
		return types.SpecialNaN
	case precision != 0:
		return types.SpecialNone
	case numVal.InfinityModifier == pgtype.Infinity:
		return types.SpecialInfinity
	case numVal.InfinityModifier == pgtype.NegativeInfinity:
		return types.SpecialNegInfinity
	}
	return types.SpecialNone
}

func infinitySpecial(inf pgtype.InfinityModifier) types.SpecialValue {
	if inf == pgtype.NegativeInfinity {
		return types.SpecialNegInfinity
	}
	return types.SpecialInfinity
}

func parseUUID(value any) (uuid.UUID, error) {
	switch v := value.(type) {
	case string:
		return uuid.Parse(v)
	case [16]byte:
		return uuid.UUID(v), nil
	case uuid.UUID:
		return v, nil
	default:
		return uuid.UUID{}, fmt.Errorf("unsupported type for UUID: %T", value)
	}
}

func parseUUIDArray(value any) ([]uuid.UUID, error) {
	switch v := value.(type) {
	case []string:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, s := range v {
			u, err := uuid.Parse(s)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID in array: %w", err)
			}
			uuids = append(uuids, u)
		}
		return uuids, nil
	case [][16]byte:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, v16 := range v {
			uuids = append(uuids, uuid.UUID(v16))
		}
		return uuids, nil
	case []uuid.UUID:
		return v, nil
	case []any:
		uuids := make([]uuid.UUID, 0, len(v))
		for _, anyVal := range v {
			if anyVal == nil {
				uuids = append(uuids, uuid.Nil)
				continue
			}
			u, err := parseUUID(anyVal)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID in array: %w", err)
			}
			uuids = append(uuids, u)
		}
		return uuids, nil
	default:
		return nil, fmt.Errorf("unsupported type for UUID array: %T", value)
	}
}

// maxTimeMicroseconds is 23:59:59.999999, the clamp target for CockroachDB's
// legal TIME '24:00:00' extreme without pgValues.time2400, shared by the snapshot and changefeed paths.
const maxTimeMicroseconds = 24*3600*1000000 - 1

// inetPrefixText renders an INET value decoded by pgx with host addresses bare ("192.168.1.5", not the
// "192.168.1.5/32" netip form), to match the changefeed path and the Postgres text convention.
func inetPrefixText(p netip.Prefix) string {
	if p.Bits() == p.Addr().BitLen() {
		return p.Addr().String()
	}
	return p.String()
}

// normalizeInetText strips full-length masks from INET text ("192.168.1.5/32"
// becomes "192.168.1.5"): CockroachDB's inet::text always renders a mask, but
// changefeed JSON and Postgres render host addresses bare, and both paths must
// deliver the same value.
func normalizeInetText(s string) string {
	if p, err := netip.ParsePrefix(s); err == nil && p.Bits() == p.Addr().BitLen() {
		return p.Addr().String()
	}
	return s
}

var timeTZLayouts = [...]string{
	"15:04:05.999999-07:00:00",
	"15:04:05.999999-07:00",
	"15:04:05.999999-0700",
	"15:04:05.999999-07",
}

func parseTimeTZ(value string) (time.Duration, error) {
	// CockroachDB permits this extreme value for time
	value = strings.Replace(value, "24:00:00", "23:59:59.999999", 1)
	for _, layout := range timeTZLayouts {
		if t, err := time.Parse(layout, value); err == nil {
			// the UTC time of day: an offset can move it past midnight either way
			const day = 24 * time.Hour
			return (t.UTC().Sub(shared.Year0000)%day + day) % day, nil
		}
	}
	return 0, fmt.Errorf("failed to parse timetz: %s", value)
}

func intervalToString(intervalObject pgtype.Interval) (string, error) {
	if !intervalObject.Valid {
		return "", fmt.Errorf("invalid interval: %v", intervalObject)
	}

	interval := datatypes.PeerDBInterval{
		Hours:   int(intervalObject.Microseconds / 3600000000),
		Minutes: int((intervalObject.Microseconds % 3600000000) / 60000000),
		Seconds: float64(intervalObject.Microseconds%60000000) / 1000000.0,
		Days:    int(intervalObject.Days),
		Years:   int(intervalObject.Months / 12),
		Months:  int(intervalObject.Months % 12),
		Valid:   intervalObject.Valid,
	}

	intervalJSON, err := json.Marshal(interval)
	if err != nil {
		return "", fmt.Errorf("failed to marshal interval: %w", err)
	}
	return string(intervalJSON), nil
}

func crdbArrayUdtToQValueKind(udtName string) types.QValueKind {
	switch strings.ToLower(strings.TrimPrefix(udtName, "_")) {
	case "int8", "oid":
		return types.QValueKindArrayInt64
	case "int4":
		return types.QValueKindArrayInt32
	case "int2":
		return types.QValueKindArrayInt16
	case "numeric", "decimal":
		return types.QValueKindArrayNumeric
	case "float8":
		return types.QValueKindArrayFloat64
	case "float4":
		return types.QValueKindArrayFloat32
	case "bool":
		return types.QValueKindArrayBoolean
	case "uuid":
		return types.QValueKindArrayUUID
	case "jsonb", "json":
		return types.QValueKindArrayJSON
	case "date":
		return types.QValueKindArrayDate
	case "timestamp":
		return types.QValueKindArrayTimestamp
	case "timestamptz":
		return types.QValueKindArrayTimestampTZ
	case "interval":
		return types.QValueKindArrayInterval
	default:
		return types.QValueKindArrayString
	}
}
