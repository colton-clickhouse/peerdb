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

// TIME '24:00:00' is clamped to 23:59:59.999999 on every destination, the same in the snapshot and changefeed paths.
func TestTime2400(t *testing.T) {
	const clamped = 24*time.Hour - time.Microsecond
	field := types.QField{Name: "col", Type: types.QValueKindTime, Nullable: true}
	pg := newPgValues(shared.InternalVersion_Latest, protos.DBType_POSTGRES)
	qv, err := qvalueFromCrdbValue(field, protos.DBType_POSTGRES, 0, pgtype.NewMap(),
		pgtype.Time{Microseconds: 24 * 3600 * 1000000, Valid: true}, pg)
	require.NoError(t, err)
	require.Equal(t, types.QValueTime{Val: clamped}, qv, "snapshot")
	for _, raw := range []string{`"24:00:00"`, `"24:00:00.000000"`, `"23:59:59.999999"`} {
		qv, err = qvalueFromChangefeedJSON(field, json.RawMessage(raw), pg)
		require.NoError(t, err)
		require.Equal(t, types.QValueTime{Val: clamped}, qv, "changefeed %s", raw)
	}
	for _, raw := range []string{`"24:00:00."`, `"24:00:00.0.0"`, `"24:00:00..0"`, `"24:00:00.000001"`, `"24:00:001"`} {
		_, err := qvalueFromChangefeedJSON(field, json.RawMessage(raw), pg)
		require.Error(t, err, raw)
	}
}

func TestNewPgValues(t *testing.T) {
	// Postgres destinations keep NaN and infinity on every version
	require.Equal(t, pgValues{specialValues: true}, newPgValues(0, protos.DBType_POSTGRES))
	// ClickHouse coerces missing times to the Unix epoch, which is its DefaultTime
	require.Equal(t, pgValues{arrayInfinity: true, defaultTime: time.Unix(0, 0).UTC()},
		newPgValues(shared.InternalVersion_Latest, protos.DBType_CLICKHOUSE))
	require.Equal(t, pgValues{defaultTime: time.Unix(0, 0).UTC()},
		newPgValues(shared.InternalVersion_ArrayConversionIntegrity-1, protos.DBType_CLICKHOUSE))
	require.Equal(t, pgValues{specialValues: true, invalidGeo: true, arrayInfinity: true},
		newPgValues(shared.InternalVersion_Latest, protos.DBType_POSTGRES))
}

// An infinite date or timestamp in a changefeed row for a destination that doesn't keep infinity (ClickHouse) gets the
// snapshot's value, NULL or the destination's default time, instead of the parse error that stopped CDC.
func TestChangefeedScalarInfinityFallback(t *testing.T) {
	pg := newPgValues(shared.InternalVersion_Latest, protos.DBType_CLICKHOUSE)
	epoch := time.Unix(0, 0).UTC()
	for _, tc := range []struct {
		kind     types.QValueKind
		raw      string
		nullable bool
		want     types.QValue
	}{
		{types.QValueKindDate, `"infinity"`, true, types.QValueNull(types.QValueKindDate)},
		{types.QValueKindDate, `"-infinity"`, false, types.QValueDate{Val: epoch}},
		{types.QValueKindTimestamp, `"infinity"`, false, types.QValueTimestamp{Val: epoch}},
		{types.QValueKindTimestampTZ, `"-infinity"`, true, types.QValueNull(types.QValueKindTimestampTZ)},
	} {
		qv, err := qvalueFromChangefeedJSON(types.QField{Name: "c", Type: tc.kind, Nullable: tc.nullable},
			json.RawMessage(tc.raw), pg)
		require.NoError(t, err, "%s %s", tc.kind, tc.raw)
		require.Equal(t, tc.want, qv, "%s %s", tc.kind, tc.raw)
	}
	// mirrors from before ArrayConversionIntegrity still fail, as they did
	_, err := qvalueFromChangefeedJSON(types.QField{Name: "c", Type: types.QValueKindDate, Nullable: true},
		json.RawMessage(`"infinity"`), newPgValues(shared.InternalVersion_ArrayConversionIntegrity-1, protos.DBType_CLICKHOUSE))
	require.Error(t, err)
}
