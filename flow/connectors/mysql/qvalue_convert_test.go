package connmysql

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// TestQkindFromMysqlColumnTypeMariaDB covers the MariaDB-only types as they appear in
// information_schema (the snapshot/QRep path), which is how these columns are typed
// since the DDL parser rejects them.
func TestQkindFromMysqlColumnTypeMariaDB(t *testing.T) {
	for _, tc := range []struct {
		dataType string
		want     types.QValueKind
	}{
		{"uuid", types.QValueKindUUID},
		{"inet4", types.QValueKindINET},
		{"inet6", types.QValueKindINET},
	} {
		t.Run(tc.dataType, func(t *testing.T) {
			qkind, err := QkindFromMysqlColumnType(tc.dataType, true, shared.InternalVersion_Latest)
			require.NoError(t, err)
			require.Equal(t, tc.want, qkind)
		})
	}
}

func TestQkindFromMysqlColumnTypeCompressed(t *testing.T) {
	for _, ct := range []string{
		"varchar(100) /*M!100301 COMPRESSED*/",
		"text /*M!100301 COMPRESSED*/",
		"blob /*M!100301 COMPRESSED*/",
	} {
		t.Run(ct, func(t *testing.T) {
			_, err := QkindFromMysqlColumnType(ct, true, shared.InternalVersion_Latest)
			require.ErrorContains(t, err, "COMPRESSED")
		})
	}
}

func TestQkindFromMysqlType_Bit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint32
		want    types.QValueKind
	}{
		{"first version maps BIT to Int64", shared.InternalVersion_First, types.QValueKindInt64},
		{
			"version just before the gate maps BIT to Int64",
			shared.InternalVersion_MySQLConvertBitToUInt64 - 1,
			types.QValueKindInt64,
		},
		{
			"gating version maps BIT to UInt64",
			shared.InternalVersion_MySQLConvertBitToUInt64,
			types.QValueKindUInt64,
		},
		{"latest version maps BIT to UInt64", shared.InternalVersion_Latest, types.QValueKindUInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qkind, err := qkindFromMysqlType(mysql.MYSQL_TYPE_BIT, false, 0, tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.want, qkind)
		})
	}
}

// Empty, non-NULL binary values must not be a nil slice (sinks write nil as NULL) from
// InternalVersion_MySQLEmptyBinaryAsEmpty on; older mirrors keep nil.
func TestQValueFromMysql_EmptyBinary(t *testing.T) {
	ev := &replication.TableMapEvent{ColumnType: []byte{mysql.MYSQL_TYPE_VARCHAR}, ColumnMeta: []uint16{16}}
	logger := log.NewStructuredLogger(nil)
	var coercionReported bool
	for _, tc := range []struct {
		version uint32
		wantNil bool
	}{
		{shared.InternalVersion_MySQLEmptyBinaryAsEmpty - 1, true},
		{shared.InternalVersion_MySQLEmptyBinaryAsEmpty, false},
	} {
		check := func(what string, qv types.QValue, err error) {
			t.Helper()
			require.NoError(t, err, what)
			b := qv.(types.QValueBytes).Val
			require.Empty(t, b, what)
			require.Equal(t, tc.wantNil, b == nil, "%s, version %d", what, tc.version)
		}
		// snapshot: go-mysql's buffer for an empty value is nil until the column has held a non-empty one
		qv, err := QValueFromMysqlFieldValue(types.QValueKindBytes, mysql.MYSQL_TYPE_VAR_STRING,
			mysql.NewFieldValue(mysql.FieldValueTypeString, 0, nil), tc.version)
		check("snapshot", qv, err)
		// cdc: VARBINARY and CHARACTER SET binary strings arrive as string, BLOBs as []byte
		qv, err = QValueFromMysqlRowEvent(ev, 0, nil, nil, types.QValueKindBytes, "", nil, logger, &coercionReported, tc.version)
		check("cdc string", qv, err)
		qv, err = QValueFromMysqlRowEvent(
			ev, 0, nil, nil, types.QValueKindBytes, []byte(nil), nil, logger, &coercionReported, tc.version)
		check("cdc []byte", qv, err)
	}
}

func TestQkindFromMysqlColumnType_Set(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata bool
		version  uint32
		want     types.QValueKind
	}{
		{"with metadata maps SET to String", true, shared.InternalVersion_Latest, types.QValueKindString},
		{"without metadata, before gate maps SET to String", false, shared.InternalVersion_MySQL5ConvertSetsToInts - 1, types.QValueKindString},
		{"without metadata, gate maps SET to Uint64Set", false, shared.InternalVersion_MySQL5ConvertSetsToInts, types.QValueKindUint64Set},
		{"without metadata, latest maps SET to Uint64Set", false, shared.InternalVersion_Latest, types.QValueKindUint64Set},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qkind, err := QkindFromMysqlColumnType("set('a','b','c')", tc.metadata, tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.want, qkind)
		})
	}
}

func TestProcessTime(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	for _, ts := range []struct {
		out time.Duration
		in  string
	}{
		{time.Date(1970, 1, 1, 23, 30, 0, 500000000, time.UTC).Sub(epoch), "23:30.5"},
		{time.Date(1970, 2, 3, 8, 0, 1, 0, time.UTC).Sub(epoch), "800:0:1"},
		{time.Date(1969, 11, 28, 15, 59, 59, 0, time.UTC).Sub(epoch), "-800:0:1"},
		{time.Date(1969, 11, 28, 15, 59, 58, 900000000, time.UTC).Sub(epoch), "-800:0:1.1"},
		{time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC).Sub(epoch), "1."},
		{time.Date(1970, 1, 1, 0, 12, 34, 0, time.UTC).Sub(epoch), "1234"},
		{time.Date(1970, 1, 1, 3, 12, 34, 0, time.UTC).Sub(epoch), "31234"},
		{time.Date(1970, 1, 1, 0, 0, 1, 120000000, time.UTC).Sub(epoch), "1.12"},
		{time.Date(1970, 1, 1, 0, 0, 1, 12000000, time.UTC).Sub(epoch), "1.012"},
		{time.Date(1970, 1, 1, 0, 0, 1, 12300000, time.UTC).Sub(epoch), "1.0123"},
		{time.Date(1970, 1, 1, 0, 0, 1, 1230000, time.UTC).Sub(epoch), "1.00123"},
		{time.Date(1970, 1, 1, 0, 0, 1, 1000, time.UTC).Sub(epoch), "1.000001"},
		{time.Date(1970, 1, 1, 0, 0, 1, 200, time.UTC).Sub(epoch), "1.0000002"},
		{time.Date(1970, 1, 1, 0, 0, 1, 30, time.UTC).Sub(epoch), "1.00000003"},
		{time.Date(1970, 1, 1, 0, 0, 1, 4, time.UTC).Sub(epoch), "1.000000004"},
		{0, "123.aa"},
		{0, "hh:00:00"},
		{0, "00:mm:00"},
		{0, "00:00:ss"},
		{0, "hh:00"},
		{0, "00:mm"},
		{0, "ss"},
		{0, "mm00"},
		{0, "00ss"},
		{0, "hh0000"},
		{0, "00mm00"},
		{0, "0000ss"},
	} {
		tm, err := processTime(ts.in)
		if tm == 0 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, ts.out, tm)
	}
}

func TestCompactMySQLJSON(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		// MySQL server-rendered text: space after ':' and ','.
		{`{"a": 1, "b": 2}`, `{"a":1,"b":2}`},
		// DOUBLE keeps its trailing zero; it is not renumbered.
		{`{"x": 1.0}`, `{"x":1.0}`},
		// Object key order is preserved (not lexicographically sorted).
		{`{"b": 1, "a": 2}`, `{"b":1,"a":2}`},
		// Nested arrays/objects.
		{`{"arr": [1, 2, {"k": "v"}]}`, `{"arr":[1,2,{"k":"v"}]}`},
		// Whitespace inside string values is untouched.
		{`{"s": "a, b: c"}`, `{"s":"a, b: c"}`},
		{`{}`, `{}`},
		{`[]`, `[]`},
	} {
		require.Equal(t, tc.want, compactMySQLJSON([]byte(tc.in)), "in=%q", tc.in)
	}

	// Invalid JSON falls back to the original bytes rather than dropping the value.
	require.Equal(t, `not json`, compactMySQLJSON([]byte(`not json`)))
}

func TestProcessGeometryData(t *testing.T) {
	qv, err := processGeometryData(nil)
	require.NoError(t, err)
	require.Equal(t, types.QValueGeometry{}, qv)

	qv, err = processGeometryData([]byte{})
	require.NoError(t, err)
	require.Equal(t, types.QValueGeometry{}, qv)

	// 1-4 bytes cannot be a valid SRID+WKB payload and is still rejected
	_, err = processGeometryData([]byte{0, 0, 0, 0})
	require.ErrorContains(t, err, "geometry data too short")

	// SRID prefix + WKB for POINT(1 2)
	point := []byte{
		0, 0, 0, 0, // SRID 0
		1,          // little-endian
		1, 0, 0, 0, // type: point
		0, 0, 0, 0, 0, 0, 0xf0, 0x3f, // x = 1
		0, 0, 0, 0, 0, 0, 0, 0x40, // y = 2
	}
	qv, err = processGeometryData(point)
	require.NoError(t, err)
	require.Equal(t, types.QValueGeometry{Val: "POINT (1 2)"}, qv)

	// the SRID stays out of the WKT (ClickHouse) and is kept beside it
	copy(point, []byte{0x11, 0x0f, 0, 0}) // SRID 3857
	qv, err = processGeometryData(point)
	require.NoError(t, err)
	require.Equal(t, types.QValueGeometry{Val: "POINT (1 2)", SRID: 3857}, qv)
}

func TestIsZeroDate(t *testing.T) {
	gate, before := shared.InternalVersion_MySQLZeroDatePartsAsZero, shared.InternalVersion_MySQLZeroDatePartsAsZero-1
	for _, tc := range []struct {
		s                string
		wantOld, wantNew bool
	}{
		{"0000-00-00", true, true},
		{"0000-00-00 00:00:00", true, true},
		{"0000-00-00 12:34:56.000001", true, true},
		{"2024-00-15", false, true},          // zero month
		{"2024-02-00 12:00:00", false, true}, // zero day
		{"2024-00-00 01:02:03.000004", false, true},
		{"0000-05-10", false, true}, // zero year: no Postgres equivalent
		{"0000-02-03 04:05:06", false, true},
		{"2024-05-15", false, false},
		{"2000-10-01 00:00:00", false, false}, // zeros elsewhere in a real date
		{"1000-01-01", false, false},
		{"", false, false},
	} {
		require.Equal(t, tc.wantOld, isZeroDate(tc.s, before), "old: %q", tc.s)
		require.Equal(t, tc.wantNew, isZeroDate(tc.s, gate), "new: %q", tc.s)
	}
}

func TestQValueFromMysql_PartialZeroDates(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	gate := shared.InternalVersion_MySQLZeroDatePartsAsZero
	ev := &replication.TableMapEvent{ColumnType: []byte{mysql.MYSQL_TYPE_DATETIME2}, ColumnMeta: []uint16{0}}
	var coercionReported bool
	snapshot := func(s string, version uint32) time.Time {
		t.Helper()
		qv, err := QValueFromMysqlFieldValue(types.QValueKindDate, 0, mysql.NewFieldValue(mysql.FieldValueTypeString, 0, []byte(s)), version)
		require.NoError(t, err, s)
		return qv.Value().(time.Time).UTC()
	}
	cdc := func(s string, version uint32) time.Time {
		t.Helper()
		qv, err := QValueFromMysqlRowEvent(ev, 0, nil, nil, types.QValueKindTimestamp, s, nil,
			log.NewStructuredLogger(nil), &coercionReported, version)
		require.NoError(t, err, s)
		return qv.Value().(time.Time).UTC()
	}

	require.Equal(t, epoch, snapshot("2024-00-15", gate))
	require.Equal(t, epoch, cdc("2024-02-00 12:00:00", gate))
	// older mirrors keep rolling a zero month or day forward
	require.Equal(t, time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), snapshot("2024-00-15", gate-1))
	require.Equal(t, time.Date(2024, 2, 1, 12, 0, 0, 0, time.UTC), cdc("2024-02-00 12:00:00", gate-1))
}
