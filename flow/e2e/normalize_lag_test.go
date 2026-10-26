package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// Pending normalize lag on catalog rows alone: only the current mirror's batches count. A mirror dropped and
// recreated under the same name keeps the old batches (unless its stats are dropped); a resync keeps the flow
// and, with stats kept, its batches, and setting it up again deletes the ones never normalized.
func TestPendingNormalizeLag(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	suffix := strings.ToLower(common.RandomString(8))
	var peerID int32
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO peers(name, type, options) VALUES ($1, 3, ''::bytea) RETURNING id`,
		"normalize_lag_"+suffix).Scan(&peerID))
	var flows []string
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.cdc_batches WHERE flow_name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.cdc_flows WHERE flow_name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM flows WHERE name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM peers WHERE id = $1`, peerID)
	})
	newFlow := func(name string, createdHoursAgo int) string {
		name = name + "_" + suffix
		flows = append(flows, name)
		_, err := pool.Exec(ctx, `INSERT INTO flows(name, source_peer, destination_peer, workflow_id, status, created_at)
			VALUES ($1::text, $2, $2, $1::text, $3, now() - make_interval(hours => $4))`,
			name, peerID, protos.FlowStatus_STATUS_RUNNING, createdHoursAgo)
		require.NoError(t, err)
		require.NoError(t, monitoring.InitializeCDCFlow(ctx, pool, name))
		return name
	}
	// an open (synced, not normalized) batch synced minutesAgo
	openBatch := func(flow string, batchID int, minutesAgo int) {
		_, err := pool.Exec(ctx, `INSERT INTO peerdb_stats.cdc_batches(flow_name, batch_id, rows_in_batch, batch_start_lsn,
			batch_end_lsn, start_time, sync_time) VALUES ($1, $2, 1, 0, 0, now() - make_interval(mins => $3),
			now() - make_interval(mins => $3))`, flow, batchID, minutesAgo)
		require.NoError(t, err)
	}
	lag := func(flow string) (int64, bool) {
		lags, err := monitoring.GetPendingNormalizeLagByFlow(ctx, pool)
		require.NoError(t, err)
		l, ok := lags[flow]
		return l, ok
	}

	// recreated an hour ago: the previous mirror's open batch from 2 hours ago doesn't count, its own does
	recreated := newFlow("recreated", 1)
	openBatch(recreated, 7, 120)
	_, ok := lag(recreated)
	require.False(t, ok, "the previous mirror's batch counts")
	openBatch(recreated, 1, 10)
	l, ok := lag(recreated)
	require.True(t, ok)
	require.InDelta(t, 10*60*1e6, float64(l), 60*1e6, "lag of the mirror's own batch")

	// resynced: the flow (and its created_at) stays, batch ids restart, and setting up the new raw table
	// deletes the batches never normalized
	resynced := newFlow("resynced", 3)
	openBatch(resynced, 40, 90)
	_, ok = lag(resynced)
	require.True(t, ok)
	require.NoError(t, monitoring.DeleteOpenCDCBatches(ctx, pool, resynced))
	_, ok = lag(resynced)
	require.False(t, ok, "a resynced mirror inherits the old open batches")
}
