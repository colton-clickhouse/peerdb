package connpostgres

import (
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func pgForArrayTests(t *testing.T) *PostgresConnector {
	t.Helper()
	if os.Getenv("PG_HOST") == "" {
		t.Skip("needs Postgres: set PG_HOST (e2e services)")
	}
	c, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// parseLive reads expr from a live Postgres in the given result format and converts it the way the snapshot and
// CDC paths do: pgx decodes the raw value for its OID, then parseFieldFromPostgresOID converts it.
func parseLive(t *testing.T, c *PostgresConnector, expr string, format int16, dst protos.DBType, version uint32,
) (types.QValue, error) {
	t.Helper()
	rows, err := c.conn.Query(t.Context(), "SELECT "+expr, pgx.QueryResultFormats{format})
	require.NoError(t, err, expr)
	defer rows.Close()
	require.True(t, rows.Next(), expr)
	fd := rows.FieldDescriptions()[0]
	typeMap := c.conn.TypeMap()
	dt, ok := typeMap.TypeForOID(fd.DataTypeOID)
	require.True(t, ok, "pgx knows OID %d", fd.DataTypeOID)
	decoded, err := dt.Codec.DecodeValue(typeMap, fd.DataTypeOID, fd.Format, slices.Clone(rows.RawValues()[0]))
	require.NoError(t, err, expr)
	return c.parseFieldFromPostgresOID(fd.DataTypeOID, fd.TypeModifier, true, dst, decoded, nil, version)
}

var formats = map[string]int16{"text": pgx.TextFormatCode, "binary": pgx.BinaryFormatCode}

// ±infinity elements of date/timestamp/timestamptz arrays arrive from pgx as pgtype.InfinityModifier, which the
// element cast turned into 0001-01-01: Postgres stores infinity, so a Postgres destination must get it back. A
// NULL element must stay NULL instead of becoming 0001-01-01 too.
func TestTimeArrayInfinityAndNull(t *testing.T) {
	c := pgForArrayTests(t)
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	specials := []types.SpecialValue{types.SpecialNone, types.SpecialInfinity, types.SpecialNegInfinity, types.SpecialNone}
	nulls := []bool{false, false, false, true}
	for _, tc := range []struct {
		expr string
		want types.QValue
	}{
		{"ARRAY['2024-01-02', 'infinity', '-infinity', NULL]::date[]",
			types.QValueArrayDate{Val: []time.Time{day, {}, {}, {}}, Nulls: nulls, Specials: specials}},
		{"ARRAY['2024-01-02', 'infinity', '-infinity', NULL]::timestamp[]",
			types.QValueArrayTimestamp{Val: []time.Time{day, {}, {}, {}}, Nulls: nulls, Specials: specials}},
		{"ARRAY['2024-01-02 00:00:00+00', 'infinity', '-infinity', NULL]::timestamptz[]",
			types.QValueArrayTimestampTZ{Val: []time.Time{day, {}, {}, {}}, Nulls: nulls, Specials: specials}},
	} {
		for name, format := range formats {
			t.Run(tc.expr+"/"+name, func(t *testing.T) {
				qv, err := parseLive(t, c, tc.expr, format, protos.DBType_POSTGRES, shared.InternalVersion_Latest)
				require.NoError(t, err)
				require.Equal(t, tc.want, normalizeTimes(qv))
			})
		}
	}
}

// normalizeTimes puts array times in UTC so text and binary decodes compare equal.
func normalizeTimes(qv types.QValue) types.QValue {
	utc := func(ts []time.Time) []time.Time {
		out := make([]time.Time, len(ts))
		for i, v := range ts {
			if !v.IsZero() {
				out[i] = v.UTC()
			}
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

// Every array type whose elements pgx already decodes as the kind's element type: the stricter element cast must
// convert them exactly as before, in both formats, so a healthy mirror can't start failing.
func TestArrayHappyPathsUnchanged(t *testing.T) {
	c := pgForArrayTests(t)
	for _, expr := range []string{
		"ARRAY[1, -2, NULL]::int2[]",
		"ARRAY[1, -2147483648, NULL]::int4[]",
		"ARRAY[1, -9223372036854775808, NULL]::int8[]",
		"ARRAY[1.5, -0.25, NULL]::float4[]",
		"ARRAY[1.5, 1e300, NULL]::float8[]",
		"ARRAY[true, false, NULL]::bool[]",
		"ARRAY[1.25, -3, NULL]::numeric[]",
		"ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, NULL]",
		"ARRAY['1 day 02:03:04'::interval, NULL]",
		"ARRAY['a', 'b,c', '', NULL]::text[]",
		"ARRAY['a', NULL]::varchar[]",
		"ARRAY['2024-01-02', NULL]::date[]",
		"ARRAY['2024-01-02 03:04:05.123456', NULL]::timestamp[]",
		"ARRAY['2024-01-02 03:04:05.123456+00', NULL]::timestamptz[]",
		"ARRAY['[1,5)'::int4range, NULL]",
		"ARRAY['12:34:56'::time, NULL]",
	} {
		t.Run(expr, func(t *testing.T) {
			text, err := parseLive(t, c, expr, pgx.TextFormatCode, protos.DBType_POSTGRES, shared.InternalVersion_Latest)
			require.NoError(t, err, "text")
			binary, err := parseLive(t, c, expr, pgx.BinaryFormatCode, protos.DBType_POSTGRES, shared.InternalVersion_Latest)
			require.NoError(t, err, "binary")
			require.Equal(t, normalizeTimes(text), normalizeTimes(binary), "text and binary decode differently")
		})
	}
}

// Older mirrors keep what they wrote before (the zero time for infinity and NULL elements, nothing marked), and
// destinations that can't store infinity or NULL elements get DefaultTime, as non-nullable scalars do.
func TestTimeArrayInfinityOldVersionAndOtherDestinations(t *testing.T) {
	c := pgForArrayTests(t)
	const expr = "ARRAY['2024-01-02', 'infinity', '-infinity', NULL]::date[]"
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	epoch := time.Unix(0, 0).UTC()
	for name, format := range formats {
		t.Run(name, func(t *testing.T) {
			qv, err := parseLive(t, c, expr, format, protos.DBType_POSTGRES, shared.InternalVersion_ArrayConversionIntegrity-1)
			require.NoError(t, err)
			require.Equal(t, types.QValueArrayDate{Val: []time.Time{day, {}, {}, {}}}, normalizeTimes(qv))

			qv, err = parseLive(t, c, expr, format, protos.DBType_CLICKHOUSE, shared.InternalVersion_Latest)
			require.NoError(t, err)
			// NULL elements too: ClickHouse doesn't store them (the mask is kept, and ignored there)
			require.Equal(t, types.QValueArrayDate{Val: []time.Time{day, epoch, epoch, epoch}, Nulls: []bool{false, false, false, true}},
				normalizeTimes(qv))
		})
	}
}
