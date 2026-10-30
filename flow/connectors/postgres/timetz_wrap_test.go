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

// A Postgres source's TIMETZ east of UTC early in the day parses to a negative offset from the day's start; the COPY
// encoder wraps it (model.timetzBinary), so it isn't rejected as out of range and retried forever.
func TestTimeTZEastOfUTCParsesBeforeTheDay(t *testing.T) {
	c := &PostgresConnector{typeMap: pgtype.NewMap(), hushWarnOID: map[uint32]struct{}{}}
	qv, err := c.parseFieldFromPostgresOID(pgtype.TimetzOID, -1, true, protos.DBType_POSTGRES, "01:00:00+05", nil,
		shared.InternalVersion_Latest)
	require.NoError(t, err)
	require.Equal(t, types.QValueTimeTZ{Val: -4 * time.Hour}, qv)
}
