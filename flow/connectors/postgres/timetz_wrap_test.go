package connpostgres

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// A Postgres source's TIMETZ whose UTC time of day falls on another day (01:00+05 is -4h from the day's start,
// 23:30-05 is 28:30) is wrapped into the day for Postgres destinations, whose timetz only takes 0 to 24:00: CDC's
// normalize cast rejected the raw value, and CDC stopped. Other destinations keep the value as before.
func TestTimeTZAcrossMidnightWrapsForPostgres(t *testing.T) {
	c := &PostgresConnector{typeMap: pgtype.NewMap(), hushWarnOID: map[uint32]struct{}{}}
	for _, tc := range []struct {
		in   string
		dst  protos.DBType
		want time.Duration
	}{
		{"01:00:00+05", protos.DBType_POSTGRES, 20 * time.Hour},
		{"23:30:00-05", protos.DBType_POSTGRES, 4*time.Hour + 30*time.Minute},
		{"12:34:56.789+00", protos.DBType_POSTGRES, 12*time.Hour + 34*time.Minute + 56789*time.Millisecond},
		{"01:00:00+05", protos.DBType_CLICKHOUSE, -4 * time.Hour},
		{"23:30:00-05", protos.DBType_CLICKHOUSE, 28*time.Hour + 30*time.Minute},
	} {
		qv, err := c.parseFieldFromPostgresOID(pgtype.TimetzOID, -1, true, tc.dst, tc.in, nil, shared.InternalVersion_Latest)
		require.NoError(t, err)
		require.Equal(t, types.QValueTimeTZ{Val: tc.want}, qv, "%s to %s", tc.in, tc.dst)
	}
}
