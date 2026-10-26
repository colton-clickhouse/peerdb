package e2e

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// The STALLED transitions on catalog rows alone: which errors start a streak, and what clears it. Flows,
// errors and batches are inserted with timestamps in the past, so no mirror has to fail for real.
func TestStalledStreak(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	suffix := strings.ToLower(common.RandomString(8))
	const threshold = 30
	// the scheduled tasks clear STALLED flows every minute with the stack's threshold, which by default (0) clears
	// all of them, these included
	SetDynamicSetting(t, "PEERDB_STALLED_THRESHOLD_MINUTES", strconv.Itoa(threshold))
	stalling := alerting.StallingErrorClassNames()

	var peerID int32
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO peers(name, type, options) VALUES ($1, 3, ''::bytea) RETURNING id`,
		"stalled_streak_"+suffix).Scan(&peerID))
	var flows []string
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.flow_errors WHERE flow_name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.cdc_batches WHERE flow_name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM peerdb_stats.cdc_flows WHERE flow_name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM flows WHERE name = ANY($1)`, flows)
		_, _ = pool.Exec(ctx, `DELETE FROM peers WHERE id = $1`, peerID)
	})
	newFlow := func(name string, status protos.FlowStatus) string {
		name = name + "_" + suffix
		flows = append(flows, name)
		_, err := pool.Exec(ctx, `INSERT INTO flows(name, source_peer, destination_peer, workflow_id, status, created_at)
			VALUES ($1::text, $2, $2, $1::text, $3, now() - interval '3 hours')`, name, peerID, status)
		require.NoError(t, err)
		// cdc_batches references it
		_, err = pool.Exec(ctx, `INSERT INTO peerdb_stats.cdc_flows(flow_name, latest_lsn_at_source, latest_lsn_at_target)
			VALUES ($1, 0, 0)`, name)
		require.NoError(t, err)
		return name
	}
	// errors of a class at each minutesAgo; class NULL is what errors recorded without a class (and older builds)
	// store
	addErrors := func(flow string, class *alerting.ErrorClass, minutesAgo ...int) {
		var name, action *string
		if class != nil {
			name, action = &class.Class, new(string(class.ErrorAction()))
		}
		for _, m := range minutesAgo {
			_, err := pool.Exec(ctx, `INSERT INTO peerdb_stats.flow_errors(flow_name, error_message, error_type, error_class,
				error_action, error_timestamp) VALUES ($1, 'test', 'error', $2, $3, now() - make_interval(mins => $4))`,
				flow, name, action, m)
			require.NoError(t, err)
		}
	}
	// a destination table changed: needs a fix. Connectivity is user-actionable too, but recovers by itself.
	notifyUser, telemetry, connectivity := &alerting.ErrorNotifyDestinationModified, &alerting.ErrorOther,
		&alerting.ErrorNotifyConnectivity
	every5 := func(from, to int) []int {
		var out []int
		for m := from; m >= to; m -= 5 {
			out = append(out, m)
		}
		return out
	}
	mark := func(flow string) bool {
		stalled, err := internal.MarkFlowStalledIfPersistent(ctx, pool, flow, threshold, stalling)
		require.NoError(t, err)
		return stalled
	}

	// transient errors for an hour, then a user-actionable one: not stalled yet, its streak just started
	transientFirst := newFlow("transient_first", protos.FlowStatus_STATUS_RUNNING)
	addErrors(transientFirst, telemetry, every5(60, 5)...)
	addErrors(transientFirst, nil, 3)
	addErrors(transientFirst, notifyUser, 0)
	require.False(t, mark(transientFirst), "transient errors must not count toward STALLED")

	// an outage: connectivity errors for 40 minutes don't make a mirror STALLED
	outage := newFlow("outage", protos.FlowStatus_STATUS_RUNNING)
	addErrors(outage, connectivity, every5(40, 0)...)
	require.False(t, mark(outage), "connectivity errors clear by themselves")

	// user-actionable errors for 40 minutes: stalled
	actionable := newFlow("actionable", protos.FlowStatus_STATUS_RUNNING)
	addErrors(actionable, notifyUser, every5(40, 0)...)
	require.True(t, mark(actionable))

	// a gap longer than the threshold restarts the streak
	gap := newFlow("gap", protos.FlowStatus_STATUS_RUNNING)
	addErrors(gap, notifyUser, 80, 20, 15, 10, 5, 0)
	require.False(t, mark(gap))

	// progress since the errors started restarts the streak
	progressed := newFlow("progressed", protos.FlowStatus_STATUS_RUNNING)
	addErrors(progressed, notifyUser, every5(60, 0)...)
	_, err = pool.Exec(ctx, `INSERT INTO peerdb_stats.cdc_batches(flow_name, batch_id, rows_in_batch, batch_start_lsn,
		batch_end_lsn, start_time, sync_time, end_time)
		VALUES ($1, 1, 1, 0, 0, now() - interval '21 minutes', now() - interval '21 minutes', now() - interval '20 minutes')`,
		progressed)
	require.NoError(t, err)
	require.False(t, mark(progressed))

	// clearing: user-actionable errors stopped 40 minutes ago, transient ones go on
	stoppedFailing := newFlow("stopped_failing", protos.FlowStatus_STATUS_STALLED)
	addErrors(stoppedFailing, notifyUser, every5(90, 40)...)
	addErrors(stoppedFailing, telemetry, every5(35, 0)...)
	// still failing with user-actionable errors, no progress
	stillFailing := newFlow("still_failing", protos.FlowStatus_STATUS_STALLED)
	addErrors(stillFailing, notifyUser, every5(90, 0)...)
	// quiet but not caught up: no error for 40 minutes, but a synced batch still waits for normalize
	quietBehind := newFlow("quiet_behind", protos.FlowStatus_STATUS_STALLED)
	addErrors(quietBehind, notifyUser, every5(90, 40)...)
	_, err = pool.Exec(ctx, `INSERT INTO peerdb_stats.cdc_batches(flow_name, batch_id, rows_in_batch, batch_start_lsn,
		batch_end_lsn, start_time, sync_time) VALUES ($1, 1, 1, 0, 0, now() - interval '50 minutes', now() - interval '50 minutes')`,
		quietBehind)
	require.NoError(t, err)
	cleared, err := internal.ClearStalledFlows(ctx, pool, threshold, stalling)
	require.NoError(t, err)
	require.True(t, slices.Contains(cleared, stoppedFailing), "cleared %v", cleared)
	require.False(t, slices.Contains(cleared, stillFailing), "cleared %v", cleared)
	require.False(t, slices.Contains(cleared, quietBehind), "cleared %v", cleared)

	for flow, want := range map[string]protos.FlowStatus{
		transientFirst: protos.FlowStatus_STATUS_RUNNING,
		actionable:     protos.FlowStatus_STATUS_STALLED,
		stoppedFailing: protos.FlowStatus_STATUS_RUNNING,
		stillFailing:   protos.FlowStatus_STATUS_STALLED,
		outage:         protos.FlowStatus_STATUS_RUNNING,
		quietBehind:    protos.FlowStatus_STATUS_STALLED,
	} {
		status, err := internal.GetWorkflowStatusByName(ctx, pool, flow)
		require.NoError(t, err)
		require.Equal(t, want, status, fmt.Sprintf("flow %s", flow))
	}
}
