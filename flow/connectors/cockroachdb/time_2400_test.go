package conncockroachdb

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// TIME '24:00:00' is kept for Postgres destinations on InternalVersion_PgTime2400, and clamped to
// 23:59:59.999999 otherwise, the same in the snapshot and changefeed paths.
func TestTime2400(t *testing.T) {
	const clamped = 24*time.Hour - time.Microsecond
	field := types.QField{Name: "col", Type: types.QValueKindTime, Nullable: true}
	typeMap := pgtype.NewMap()
	for _, tc := range []struct {
		name string
		pg   pgValues
		want time.Duration
	}{
		{"kept", pgValues{time2400: true}, 24 * time.Hour},
		{"clamped", pgValues{}, clamped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qv, err := qvalueFromCrdbValue(field, protos.DBType_POSTGRES, 0, typeMap,
				pgtype.Time{Microseconds: 24 * 3600 * 1000000, Valid: true}, tc.pg)
			require.NoError(t, err)
			require.Equal(t, types.QValueTime{Val: tc.want}, qv, "snapshot")
			for _, raw := range []string{`"24:00:00"`, `"24:00:00.000000"`} {
				qv, err = qvalueFromChangefeedJSON(field, json.RawMessage(raw), tc.pg)
				require.NoError(t, err)
				require.Equal(t, types.QValueTime{Val: tc.want}, qv, "changefeed %s", raw)
			}
		})
	}
	for _, raw := range []string{`"24:00:00."`, `"24:00:00.0.0"`, `"24:00:00..0"`, `"24:00:00.000001"`, `"24:00:001"`} {
		_, err := qvalueFromChangefeedJSON(field, json.RawMessage(raw), pgValues{time2400: true})
		require.Error(t, err, raw)
	}
	qv, err := qvalueFromChangefeedJSON(field, json.RawMessage(`"23:59:59.999999"`), pgValues{time2400: true})
	require.NoError(t, err)
	require.Equal(t, types.QValueTime{Val: clamped}, qv)
}

func TestNewPgValues(t *testing.T) {
	require.Equal(t, pgValues{specialValues: true, time2400: true},
		newPgValues(shared.InternalVersion_PgTime2400, protos.DBType_POSTGRES))
	require.Equal(t, pgValues{specialValues: true},
		newPgValues(shared.InternalVersion_PgSpecialValues, protos.DBType_POSTGRES))
	// ClickHouse coerces missing times to the Unix epoch, which is its DefaultTime
	require.Equal(t, pgValues{arrayInfinity: true, defaultTime: time.Unix(0, 0).UTC()},
		newPgValues(shared.InternalVersion_Latest, protos.DBType_CLICKHOUSE))
	require.Equal(t, pgValues{defaultTime: time.Unix(0, 0).UTC()},
		newPgValues(shared.InternalVersion_ArrayConversionIntegrity-1, protos.DBType_CLICKHOUSE))
	require.Equal(t, pgValues{specialValues: true, time2400: true, invalidGeo: true, arrayInfinity: true},
		newPgValues(shared.InternalVersion_ArrayConversionIntegrity, protos.DBType_POSTGRES))
}
