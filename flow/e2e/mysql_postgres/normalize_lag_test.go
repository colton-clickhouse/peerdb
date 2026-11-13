//go:build tilt

package mysql_postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// Pending normalize lag on catalog rows alone. A mirror dropped without its stats and recreated under the same
// name keeps the old batches, and a resync keeps the flow and its batches: setting up the raw table again deletes
// the ones never normalized, and the mirror's own batches count from there.
func TestPendingNormalizeLag(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	flow := "normalize_lag_" + strings.ToLower(common.RandomString(8))
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.cdc_batches WHERE flow_name = $1`, flow)
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.cdc_flows WHERE flow_name = $1`, flow)
	})
	require.NoError(t, monitoring.InitializeCDCFlow(ctx, pool, flow))
	// a batch synced minutesAgo, normalized if normalized
	batch := func(batchID int, minutesAgo int, normalized bool) {
		_, err := pool.Exec(ctx, `INSERT INTO peerdb_stats.cdc_batches(flow_name, batch_id, rows_in_batch, batch_start_lsn,
			batch_end_lsn, start_time, sync_time, end_time) VALUES ($1, $2, 1, 0, 0, now() - make_interval(mins => $3),
			now() - make_interval(mins => $3), CASE WHEN $4 THEN now() END)`, flow, batchID, minutesAgo, normalized)
		require.NoError(t, err)
	}
	lag := func() (int64, bool) {
		lags, err := monitoring.GetPendingNormalizeLagByFlow(ctx, pool)
		require.NoError(t, err)
		l, ok := lags[flow]
		return l, ok
	}

	// the previous mirror's (or the mirror before its resync) batches: one normalized, one open
	batch(39, 150, true)
	batch(40, 120, false)
	l, ok := lag()
	require.True(t, ok)
	require.InDelta(t, 120*60*1e6, float64(l), 60*1e6, "lag of the open batch")

	require.NoError(t, monitoring.DeleteOpenCDCBatches(ctx, pool, flow))
	_, ok = lag()
	require.False(t, ok, "the recreated or resynced mirror inherits the old open batch")
	var normalized int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM peerdb_stats.cdc_batches WHERE flow_name = $1 AND end_time IS NOT NULL`, flow).Scan(&normalized))
	require.Equal(t, 1, normalized, "normalized batches are kept")

	// batch ids restart; the mirror's own open batch counts
	batch(1, 10, false)
	l, ok = lag()
	require.True(t, ok)
	require.InDelta(t, 10*60*1e6, float64(l), 60*1e6, "lag of the mirror's own batch")
}

// The normalize-gap alert for batches waiting for normalize (non-Postgres sources) reaches the catalog's alerts,
// with the text for waiting batches. The alert config only covers this flow and sends nowhere (no channels).
func TestNormalizeGapAlertForPendingBatches(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	flow := "normalize_gap_" + strings.ToLower(common.RandomString(8))
	var configID int64
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO peerdb_stats.alerting_config(service_type, service_config, alert_for_mirrors)
		VALUES ('slack', convert_to('{"auth_token":"","channel_ids":[]}', 'utf8'), ARRAY[$1]) RETURNING id`, flow).Scan(&configID))
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.alerts_v1 WHERE alert_config_id = $1`, configID)
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.alerting_config WHERE id = $1`, configID)
	})
	threshold, err := internal.PeerDBIntervalSinceLastNormalizeThresholdMinutes(ctx, nil)
	require.NoError(t, err)
	require.NotZero(t, threshold, "normalize-gap alert disabled")

	alerting.NewAlerter(ctx, pool, nil).AlertIfTooLongSinceLastNormalize(ctx,
		&alerting.AlertKeys{FlowName: flow, PendingBatches: true}, time.Duration(threshold+1)*time.Minute)
	rows, err := pool.Query(ctx, `SELECT alert_message FROM peerdb_stats.alerts_v1 WHERE alert_config_id = $1`, configID)
	require.NoError(t, err)
	messages, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Contains(t, messages[0], "has been waiting")
	require.Contains(t, messages[0], flow)
}
