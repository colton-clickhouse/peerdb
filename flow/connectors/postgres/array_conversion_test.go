package connpostgres

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Without a server: the decoded elements pgx produces for date/timestamp/numeric arrays, through
// parseFieldFromPostgresOID for every version and destination that behave differently.
func TestArrayConversionByVersionAndDestination(t *testing.T) {
	c := &PostgresConnector{typeMap: pgtype.NewMap(), hushWarnOID: map[uint32]struct{}{}}
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	epoch := time.Unix(0, 0).UTC()
	elems := func() []any { return []any{day, pgtype.Infinity, pgtype.NegativeInfinity, nil} }
	latest, old := shared.InternalVersion_Latest, shared.InternalVersion_ArrayConversionIntegrity-1
	marks := []types.SpecialValue{types.SpecialNone, types.SpecialInfinity, types.SpecialNegInfinity, types.SpecialNone}
	nulls := []bool{false, false, false, true}

	for _, tc := range []struct {
		name    string
		oid     uint32
		dst     protos.DBType
		version uint32
		want    types.QValue
	}{
		// Postgres stores infinity and NULL elements
		{"date pg", pgtype.DateArrayOID, protos.DBType_POSTGRES, latest,
			types.QValueArrayDate{Val: []time.Time{day, {}, {}, {}}, Nulls: nulls, Specials: marks}},
		{"timestamp pg", pgtype.TimestampArrayOID, protos.DBType_POSTGRES, latest,
			types.QValueArrayTimestamp{Val: []time.Time{day, {}, {}, {}}, Nulls: nulls, Specials: marks}},
		{"timestamptz pg", pgtype.TimestamptzArrayOID, protos.DBType_POSTGRES, latest,
			types.QValueArrayTimestampTZ{Val: []time.Time{day, {}, {}, {}}, Nulls: nulls, Specials: marks}},
		// ClickHouse stores neither: both become its DefaultTime, the epoch (a zero time wraps to 1974-10-01 there);
		// the NULL mask is still set, as for every source, and ClickHouse ignores it
		{"date clickhouse", pgtype.DateArrayOID, protos.DBType_CLICKHOUSE, latest,
			types.QValueArrayDate{Val: []time.Time{day, epoch, epoch, epoch}, Nulls: nulls}},
		{"timestamptz clickhouse", pgtype.TimestamptzArrayOID, protos.DBType_CLICKHOUSE, latest,
			types.QValueArrayTimestampTZ{Val: []time.Time{day, epoch, epoch, epoch}, Nulls: nulls}},
		// older mirrors keep the zero time for both, whatever the destination
		{"date pg old", pgtype.DateArrayOID, protos.DBType_POSTGRES, old,
			types.QValueArrayDate{Val: []time.Time{day, {}, {}, {}}}},
		{"date clickhouse old", pgtype.DateArrayOID, protos.DBType_CLICKHOUSE, old,
			types.QValueArrayDate{Val: []time.Time{day, {}, {}, {}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qv, err := c.parseFieldFromPostgresOID(tc.oid, -1, true, tc.dst, elems(), nil, tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.want, qv)
		})
	}

	nan := pgtype.Numeric{NaN: true, Valid: true}
	inf := pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}
	oneHalf := pgtype.Numeric{Int: decimal.RequireFromString("15").BigInt(), Exp: -1, Valid: true}
	numeric := func() []any { return []any{oneHalf, nil, nan, inf} }
	t.Run("numeric pg", func(t *testing.T) {
		qv, err := c.parseFieldFromPostgresOID(pgtype.NumericArrayOID, -1, true, protos.DBType_POSTGRES, numeric(), nil, latest)
		require.NoError(t, err)
		arr := qv.(types.QValueArrayNumeric)
		require.Equal(t, []types.SpecialValue{types.SpecialNone, types.SpecialNone, types.SpecialNaN, types.SpecialInfinity},
			arr.Specials)
		require.Equal(t, []bool{false, true, false, false}, arr.Nulls)
		require.True(t, arr.Val[0].Equal(decimal.RequireFromString("1.5")))
	})
	t.Run("numeric clickhouse and old", func(t *testing.T) {
		for _, tc := range []struct {
			dst     protos.DBType
			version uint32
		}{{protos.DBType_CLICKHOUSE, latest}, {protos.DBType_POSTGRES, old}} {
			qv, err := c.parseFieldFromPostgresOID(pgtype.NumericArrayOID, -1, true, tc.dst, numeric(), nil, tc.version)
			require.NoError(t, err)
			require.Nil(t, qv.(types.QValueArrayNumeric).Specials, "%v at %d", tc.dst, tc.version)
		}
	})
}
