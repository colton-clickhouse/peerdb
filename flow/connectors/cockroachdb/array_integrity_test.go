package conncockroachdb

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// liveArrayCase is one array value read back from a live CockroachDB the way the snapshot reads it: the query's
// raw value decoded by pgx for its wire OID, then converted for the column's stored kind. For values the
// changefeed can carry, the same value is also converted from its JSON form (to_json, the encoding changefeeds
// emit), so the two paths can be compared.
type liveArrayCase struct {
	name string
	expr string // a SQL expression of the array type
	udt  string // the column's udt_name, which the stored schema kind comes from
}

func crdbForArrayTests(t *testing.T) *CockroachDBConnector {
	t.Helper()
	if os.Getenv("CI_COCKROACH_HOST") == "" {
		t.Skip("needs CockroachDB: set CI_COCKROACH_HOST (e2e services)")
	}
	c, err := NewCockroachDBConnector(t.Context(), nil, internal.GetCockroachDBConfigFromEnv())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// snapshotAndChangefeed converts tc's value on both paths for a Postgres destination at the latest version.
func snapshotAndChangefeed(t *testing.T, c *CockroachDBConnector, tc liveArrayCase) (types.QValue, error, types.QValue, error) {
	t.Helper()
	return snapshotAndChangefeedFor(t, c, tc, protos.DBType_POSTGRES)
}

func snapshotAndChangefeedFor(t *testing.T, c *CockroachDBConnector, tc liveArrayCase, dst protos.DBType,
) (types.QValue, error, types.QValue, error) {
	t.Helper()
	rows, err := c.conn.Query(t.Context(), "SELECT "+tc.expr+", to_json("+tc.expr+")::STRING")
	require.NoError(t, err, tc.expr)
	defer rows.Close()
	require.True(t, rows.Next(), tc.expr)
	fds := rows.FieldDescriptions()
	raw := rows.RawValues()
	field := types.QField{Name: tc.name, Type: crdbArrayUdtToQValueKind(tc.udt), Nullable: true}
	pg := newPgValues(shared.InternalVersion_Latest, dst)
	typeMap := c.conn.TypeMap()
	dt, ok := typeMap.TypeForOID(fds[0].DataTypeOID)
	require.True(t, ok, "pgx knows OID %d", fds[0].DataTypeOID)
	decoded, err := dt.Codec.DecodeValue(typeMap, fds[0].DataTypeOID, fds[0].Format, slices.Clone(raw[0]))
	require.NoError(t, err, tc.expr)
	snap, snapErr := qvalueFromCrdbValue(field, dst, fds[0].DataTypeOID, typeMap, decoded, pg)
	cdc, cdcErr := qvalueFromChangefeedJSON(field, json.RawMessage(raw[1]), pg)
	return utcTimes(snap), snapErr, utcTimes(cdc), cdcErr
}

// Arrays whose snapshot value was silently wrong: every OID[] element 0 (pgx decodes oid as uint32, the kind is
// ArrayInt64), TIME[] and VARBIT[] elements in Go's fmt formatting, and ±infinity dates as 0001-01-01. The
// snapshot must produce what the changefeed produces.
func TestArraySnapshotMatchesChangefeed(t *testing.T) {
	c := crdbForArrayTests(t)
	for _, tc := range []liveArrayCase{
		{"oid", "ARRAY[1, 2147483648, 4294967295]::OID[]", "_oid"},
		{"time", "ARRAY['12:34:56', '00:00:01.5', NULL]::TIME[]", "_time"},
		{"varbit", "ARRAY[B'101', B'1', NULL]::VARBIT[]", "_varbit"},
		{"bit", "ARRAY[B'101', B'010']::BIT(3)[]", "_bit"},
		{"date_infinity", "ARRAY['2024-01-02', 'infinity', '-infinity', NULL]::DATE[]", "_date"},
		{"timestamp_infinity", "ARRAY['2024-01-02 03:04:05', 'infinity', '-infinity', NULL]::TIMESTAMP[]", "_timestamp"},
		{"timestamptz_infinity", "ARRAY['2024-01-02 03:04:05+00', 'infinity', '-infinity', NULL]::TIMESTAMPTZ[]",
			"_timestamptz"},
		{"char", `ARRAY['a', 'b', NULL]::"char"[]`, "_char"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, snapErr, cdc, cdcErr := snapshotAndChangefeed(t, c, tc)
			require.NoError(t, snapErr, "snapshot")
			require.NoError(t, cdcErr, "changefeed")
			require.Equal(t, cdc, snap, "the snapshot value differs from the changefeed's for %s", tc.expr)
		})
	}
	snap, _, _, _ := snapshotAndChangefeed(t, c, liveArrayCase{"oid", "ARRAY[1, 2147483648, 4294967295]::OID[]", "_oid"})
	require.Equal(t, types.QValueArrayInt64{Val: []int64{1, 2147483648, 4294967295}}, snap)
}

// The arrays whose elements pgx already decodes as the kind's element type: the stricter element cast must leave
// them unchanged (a healthy mirror must not start failing), and snapshot and changefeed agree.
func TestArrayHappyPathsUnchanged(t *testing.T) {
	c := crdbForArrayTests(t)
	for _, tc := range []liveArrayCase{
		{"int2", "ARRAY[1, -2, NULL]::INT2[]", "_int2"},
		{"int4", "ARRAY[1, -2147483648, NULL]::INT4[]", "_int4"},
		{"int8", "ARRAY[1, -9223372036854775808, NULL]::INT8[]", "_int8"},
		{"float4", "ARRAY[1.5, -0.25, NULL]::FLOAT4[]", "_float4"},
		{"float8", "ARRAY[1.5, 1e300, NULL]::FLOAT8[]", "_float8"},
		{"bool", "ARRAY[true, false, NULL]::BOOL[]", "_bool"},
		{"numeric", "ARRAY[1.25, -3, NULL]::DECIMAL[]", "_numeric"},
		{"uuid", "ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID, NULL]", "_uuid"},
		{"interval", "ARRAY['1 day 02:03:04'::INTERVAL, NULL]", "_interval"},
		{"string", "ARRAY['a', 'b,c', '', NULL]::STRING[]", "_text"},
		{"date", "ARRAY['2024-01-02', NULL]::DATE[]", "_date"},
		{"timestamp", "ARRAY['2024-01-02 03:04:05.123456', NULL]::TIMESTAMP[]", "_timestamp"},
		{"timestamptz", "ARRAY['2024-01-02 03:04:05.123456+00', NULL]::TIMESTAMPTZ[]", "_timestamptz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, snapErr, cdc, cdcErr := snapshotAndChangefeed(t, c, tc)
			require.NoError(t, snapErr, "snapshot")
			require.NoError(t, cdcErr, "changefeed")
			require.Equal(t, cdc, snap, "%s", tc.expr)
		})
	}
}

// A column whose type changed since the schema was stored (INT8[] data under a stored INT4[] kind) must fail
// loudly instead of turning every element into 0.
func TestArrayStaleKindFails(t *testing.T) {
	c := crdbForArrayTests(t)
	rows, err := c.conn.Query(t.Context(), "SELECT ARRAY[1, 2]::INT8[], ARRAY['x']::STRING[]")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	fds := rows.FieldDescriptions()
	typeMap := c.conn.TypeMap()
	for i, stored := range []types.QValueKind{types.QValueKindArrayInt32, types.QValueKindArrayInt64} {
		dt, ok := typeMap.TypeForOID(fds[i].DataTypeOID)
		require.True(t, ok)
		decoded, err := dt.Codec.DecodeValue(typeMap, fds[i].DataTypeOID, fds[i].Format, slices.Clone(rows.RawValues()[i]))
		require.NoError(t, err)
		qv, err := qvalueFromCrdbValue(types.QField{Name: "c", Type: stored, Nullable: true}, protos.DBType_POSTGRES,
			fds[i].DataTypeOID, typeMap, decoded, newPgValues(shared.InternalVersion_Latest, protos.DBType_POSTGRES))
		require.Error(t, err, "%s data read as %s gave %v", pgtypeName(typeMap, fds[i].DataTypeOID), stored, qv)
	}
}

func pgtypeName(m *pgtype.Map, oid uint32) string {
	if dt, ok := m.TypeForOID(oid); ok {
		return dt.Name
	}
	return "unknown"
}

// utcTimes puts array times in UTC: pgx and the changefeed parser return the same instants with different
// *time.Location values (UTC vs nil), which compare unequal as structs.
func utcTimes(qv types.QValue) types.QValue {
	utc := func(ts []time.Time) []time.Time {
		out := make([]time.Time, len(ts))
		for i, v := range ts {
			out[i] = v.UTC()
		}
		return out
	}
	switch v := qv.(type) {
	case types.QValueArrayDate:
		v.Val = utc(v.Val)
		return v
	case types.QValueArrayTimestamp:
		v.Val = utc(v.Val)
		return v
	case types.QValueArrayTimestampTZ:
		v.Val = utc(v.Val)
		return v
	}
	return qv
}

// Destinations without infinity or NULL elements (ClickHouse) get DefaultTime for both, the same on both paths.
func TestTimeArraySnapshotMatchesChangefeedClickHouse(t *testing.T) {
	c := crdbForArrayTests(t)
	for _, tc := range []liveArrayCase{
		{"date", "ARRAY['2024-01-02', 'infinity', '-infinity', NULL]::DATE[]", "_date"},
		{"timestamp", "ARRAY['2024-01-02 03:04:05', 'infinity', '-infinity', NULL]::TIMESTAMP[]", "_timestamp"},
		{"timestamptz", "ARRAY['2024-01-02 03:04:05+00', 'infinity', '-infinity', NULL]::TIMESTAMPTZ[]", "_timestamptz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, snapErr, cdc, cdcErr := snapshotAndChangefeedFor(t, c, tc, protos.DBType_CLICKHOUSE)
			require.NoError(t, snapErr, "snapshot")
			require.NoError(t, cdcErr, "changefeed")
			require.Equal(t, cdc, snap)
			epoch := time.Unix(0, 0).UTC()
			for i, v := range arrayTimes(snap) {
				if i > 0 {
					require.Equal(t, epoch, v, "element %d", i)
				}
			}
		})
	}
}

// A known gap the element cast can't see: a stale kind whose elements decode as the same Go type (DATE[] data under
// a stored TIMESTAMPTZ[] kind, both time.Time) converts without an error.
func TestArrayStaleKindSameGoTypeNotDetected(t *testing.T) {
	c := crdbForArrayTests(t)
	rows, err := c.conn.Query(t.Context(), "SELECT ARRAY['2024-01-02']::DATE[]")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	fd := rows.FieldDescriptions()[0]
	typeMap := c.conn.TypeMap()
	dt, _ := typeMap.TypeForOID(fd.DataTypeOID)
	decoded, err := dt.Codec.DecodeValue(typeMap, fd.DataTypeOID, fd.Format, slices.Clone(rows.RawValues()[0]))
	require.NoError(t, err)
	_, err = qvalueFromCrdbValue(types.QField{Name: "c", Type: types.QValueKindArrayTimestampTZ, Nullable: true},
		protos.DBType_POSTGRES, fd.DataTypeOID, typeMap, decoded, newPgValues(shared.InternalVersion_Latest, protos.DBType_POSTGRES))
	require.NoError(t, err, "not detected: both kinds decode as time.Time")
}

func arrayTimes(qv types.QValue) []time.Time {
	switch v := qv.(type) {
	case types.QValueArrayDate:
		return v.Val
	case types.QValueArrayTimestamp:
		return v.Val
	case types.QValueArrayTimestampTZ:
		return v.Val
	}
	return nil
}
