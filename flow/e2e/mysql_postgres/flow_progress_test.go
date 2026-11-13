//go:build tilt

package mysql_postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// The no-progress rule on catalog rows alone: a mirror is failing without progress once it has logged an error
// after its last progress and longer than the threshold ago, so an idle mirror's first error doesn't count. Its
// initial snapshot's progress and errors (clone flows) are its own.
func TestFlowProgress(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	suffix := strings.ToLower(common.RandomString(8))
	var peerID int32
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO peers(name, type, options) VALUES ($1, 3, ''::bytea) RETURNING id`,
		"flow_progress_"+suffix).Scan(&peerID))
	var flows []string
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.flow_errors WHERE flow_name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.qrep_runs WHERE flow_name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.cdc_flows WHERE flow_name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM flows WHERE name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM peers WHERE id = $1`, peerID)
	})
	newFlow := func(name string, createdMinutesAgo int) string {
		name = name + "_" + suffix
		flows = append(flows, name)
		_, err := pool.Exec(ctx, `INSERT INTO flows(name, source_peer, destination_peer, workflow_id, status, created_at)
			VALUES ($1::text, $2, $2, $1::text, $3, now() - make_interval(mins => $4))`,
			name, peerID, protos.FlowStatus_STATUS_RUNNING, createdMinutesAgo)
		require.NoError(t, err)
		require.NoError(t, monitoring.InitializeCDCFlow(ctx, pool, name))
		return name
	}
	normalizedBatch := func(flow string, minutesAgo int) {
		_, err := pool.Exec(ctx, `INSERT INTO peerdb_stats.cdc_batches(flow_name, batch_id, rows_in_batch, batch_start_lsn,
			batch_end_lsn, start_time, sync_time, end_time) VALUES ($1, 1, 1, 0, 0, now() - make_interval(mins => $2),
			now() - make_interval(mins => $2), now() - make_interval(mins => $2))`, flow, minutesAgo)
		require.NoError(t, err)
	}
	flowError := func(flow string, minutesAgo int, message string) {
		_, err := pool.Exec(ctx, `INSERT INTO peerdb_stats.flow_errors(flow_name, error_message, error_type, error_timestamp)
			VALUES ($1, $2, 'error', now() - make_interval(mins => $3))`, flow, message, minutesAgo)
		require.NoError(t, err)
	}
	const threshold = 30 * time.Minute
	progress := func(flow string, threshold time.Duration) internal.FlowProgress {
		p, err := internal.GetFlowProgress(ctx, pool, flow, threshold)
		require.NoError(t, err)
		return p
	}

	// idle for two days: its first error doesn't count, one from 40 minutes ago does
	idle := newFlow("idle", 3*24*60)
	normalizedBatch(idle, 2*24*60)
	flowError(idle, 0, "first error")
	p := progress(idle, threshold)
	require.False(t, p.FailingWithoutProgress, "an idle mirror's first error")
	require.InDelta(t, (48 * time.Hour).Seconds(), p.Since.Seconds(), 60)
	flowError(idle, 40, "older error")
	require.True(t, progress(idle, threshold).FailingWithoutProgress)
	require.False(t, progress(idle, 0).FailingWithoutProgress, "threshold 0 disables")

	// errors before its last progress don't count
	recovered := newFlow("recovered", 3*60)
	flowError(recovered, 120, "before progress")
	normalizedBatch(recovered, 10)
	flowError(recovered, 0, "after progress")
	p = progress(recovered, threshold)
	require.False(t, p.FailingWithoutProgress)
	require.InDelta(t, (10 * time.Minute).Seconds(), p.Since.Seconds(), 60)

	// in its initial snapshot: its clone flow's errors and finished partitions are the mirror's
	snapshot := newFlow("snapshot", 60)
	clone := "clone_" + snapshot + "_t"
	flows = append(flows, clone)
	_, err = pool.Exec(ctx, `INSERT INTO peerdb_stats.qrep_runs(flow_name, run_uuid, start_time, parent_mirror_name)
		VALUES ($1, $1, now() - interval '50 minutes', $2)`, clone, snapshot)
	require.NoError(t, err)
	flowError(clone, 40, "partition error")
	require.True(t, progress(snapshot, threshold).FailingWithoutProgress, "a failing snapshot")
	message, _, ok, err := internal.FlowLastError(ctx, pool, snapshot)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "partition error", message)
	_, err = pool.Exec(ctx, `INSERT INTO peerdb_stats.qrep_partitions(flow_name, run_uuid, partition_uuid, partition_start,
		partition_end, rows_in_partition, start_time, end_time, restart_count, parent_mirror_name)
		VALUES ($1, $1, 'p1', '', '', 1, now() - interval '10 minutes', now() - interval '5 minutes', 0, $2)`, clone, snapshot)
	require.NoError(t, err)
	p = progress(snapshot, threshold)
	require.False(t, p.FailingWithoutProgress, "a snapshot partition finished since")
	require.InDelta(t, (5 * time.Minute).Seconds(), p.Since.Seconds(), 60)
}
