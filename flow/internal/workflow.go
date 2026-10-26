package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func GetWorkflowStatus(ctx context.Context, pool shared.CatalogPool,
	workflowID string,
) (protos.FlowStatus, error) {
	var flowStatus protos.FlowStatus
	err := pool.QueryRow(ctx, "SELECT status FROM flows WHERE workflow_id = $1", workflowID).Scan(&flowStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			slog.WarnContext(ctx, "workflowId not found in catalog, will raise an error",
				slog.String("workflowId", workflowID))
			return protos.FlowStatus_STATUS_UNKNOWN, fmt.Errorf("workflowId not found in catalog: %w", err)
		}
		slog.ErrorContext(ctx, "failed to get status for flow from catalog",
			slog.Any("error", err),
			slog.String("flowID", workflowID))
		return flowStatus, fmt.Errorf("failed ot get status for flow from catalog: %w", err)
	} else if flowStatus == protos.FlowStatus_STATUS_UNKNOWN {
		slog.WarnContext(ctx, "flow status from catalog is unknown", slog.String("flowID", workflowID))
	}
	return flowStatus, nil
}

func GetWorkflowStatusByName(ctx context.Context, pool shared.CatalogPool,
	flowName string,
) (protos.FlowStatus, error) {
	var flowStatus protos.FlowStatus
	err := pool.QueryRow(ctx, "SELECT status FROM flows WHERE name = $1", flowName).Scan(&flowStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			slog.WarnContext(ctx, "flowName not found in catalog, will raise an error",
				slog.String("flowName", flowName))
			return protos.FlowStatus_STATUS_UNKNOWN, fmt.Errorf("flowName not found in catalog: %w", err)
		}
		slog.ErrorContext(ctx, "failed to get status for flow from catalog",
			slog.Any("error", err),
			slog.String("flowName", flowName))
		return flowStatus, fmt.Errorf("failed to get status for flow from catalog: %w", err)
	} else if flowStatus == protos.FlowStatus_STATUS_UNKNOWN {
		slog.WarnContext(ctx, "flow status from catalog is unknown", slog.String("flowName", flowName))
	}
	return flowStatus, nil
}

func UpdateFlowStatusInCatalog(ctx context.Context, pool shared.CatalogPool,
	workflowID string, status protos.FlowStatus,
) (protos.FlowStatus, error) {
	if _, err := pool.Exec(ctx, "UPDATE flows SET status=$1,updated_at=now() WHERE workflow_id=$2"+keepStalled(status),
		status, workflowID); err != nil {
		slog.ErrorContext(ctx, "failed to update flow status", slog.Any("error", err), slog.String("flowID", workflowID))
		return status, fmt.Errorf("failed to update flow status: %w", err)
	}
	return status, nil
}

func UpdateFlowStatusWithNameInCatalog(ctx context.Context, pool shared.CatalogPool,
	flowName string, status protos.FlowStatus,
) (protos.FlowStatus, error) {
	if _, err := pool.Exec(ctx, "UPDATE flows SET status=$1,updated_at=now() WHERE name=$2"+keepStalled(status),
		status, flowName); err != nil {
		slog.ErrorContext(ctx, "failed to update flow status", slog.Any("error", err), slog.String("flowName", flowName))
		return status, fmt.Errorf("failed to update flow status: %w", err)
	}
	return status, nil
}

// keepStalled is the WHERE condition that keeps a STALLED flow STALLED when it's set to RUNNING: the CDC workflow
// re-asserts RUNNING (e.g. at every continue-as-new), and only ClearStalledFlows clears STALLED.
func keepStalled(status protos.FlowStatus) string {
	if status != protos.FlowStatus_STATUS_RUNNING {
		return ""
	}
	return " AND status<>" + strconv.Itoa(int(protos.FlowStatus_STATUS_STALLED))
}

// lastProgressSQL is when the flow f last made progress: its newest normalized batch (every destination sets
// cdc_batches.end_time once a batch is normalized), or its creation if it hasn't yet. Only the current mirror
// counts: dropping a mirror can keep its batches and errors, which one recreated under the same name mustn't
// inherit.
const lastProgressSQL = `coalesce((SELECT max(b.end_time) FROM peerdb_stats.cdc_batches b
	WHERE b.flow_name = f.name AND b.sync_time >= f.created_at), f.created_at)`

// stallingErrorSQL selects the flow_errors rows e that count for STALLED: errors of the classes in $N, the ones
// that don't clear by themselves (alerting.StallingErrorClassNames). Transient errors don't start or extend a
// streak.
func stallingErrorSQL(n int) string {
	return fmt.Sprintf("e.error_type = 'error' AND e.error_class = ANY($%d)", n)
}

// pendingBatchesSQL: the flow f has synced batches of the current mirror that aren't normalized yet
const pendingBatchesSQL = `EXISTS (SELECT 1 FROM peerdb_stats.cdc_batches b
	WHERE b.flow_name = f.name AND b.sync_time >= f.created_at AND b.end_time IS NULL)`

// retryGapSQL is the longest gap between two errors of one ongoing failure: the threshold, but at least
// 15 minutes, since the CDC flow waits up to 10 minutes before retrying a failed sync (cdc_flow.go). $N is
// the threshold in minutes.
func retryGapSQL(n int) string {
	return fmt.Sprintf("make_interval(mins => greatest($%d, 15))", n)
}

// MarkFlowStalledIfPersistent sets a RUNNING CDC flow to STALLED if it has failed with errors of stallingClasses
// without progress for longer than thresholdMinutes: those errors since the last progress form one streak,
// without a gap longer than retryGapSQL, that started before it. Called when such an error is recorded. Returns
// whether the flow became STALLED.
func MarkFlowStalledIfPersistent(ctx context.Context, pool shared.CatalogPool, flowName string, thresholdMinutes uint32,
	stallingClasses []string,
) (bool, error) {
	tag, err := pool.Exec(ctx, `UPDATE flows f SET status = $1, updated_at = now()
		WHERE f.name = $2 AND f.status = $3 AND f.query_string IS NULL AND (
			SELECT max(e.t) FILTER (WHERE e.gap IS NULL OR e.gap > `+retryGapSQL(4)+`)
			FROM (SELECT error_timestamp AS t, error_timestamp - lag(error_timestamp) OVER (ORDER BY error_timestamp) AS gap
				FROM peerdb_stats.flow_errors e
				WHERE e.flow_name = f.name AND `+stallingErrorSQL(5)+` AND e.error_timestamp > `+lastProgressSQL+`) e
		) < now() - make_interval(mins => $4)`,
		protos.FlowStatus_STATUS_STALLED, flowName, protos.FlowStatus_STATUS_RUNNING, int32(thresholdMinutes), stallingClasses)
	if err != nil {
		return false, fmt.Errorf("failed to mark flow %s stalled: %w", flowName, err)
	}
	return tag.RowsAffected() > 0, nil
}

// ClearStalledFlows sets STALLED flows back to RUNNING once they've made progress within the last
// thresholdMinutes, or stopped failing: no error of stallingClasses within retryGapSQL and nothing synced waiting
// for normalize (a flow that merely went quiet while still broken stays STALLED). All of them when the STALLED
// status is disabled (thresholdMinutes 0). Returns the flows it cleared.
func ClearStalledFlows(ctx context.Context, pool shared.CatalogPool, thresholdMinutes uint32, stallingClasses []string,
) ([]string, error) {
	rows, err := pool.Query(ctx, `UPDATE flows f SET status = $1, updated_at = now()
		WHERE f.status = $2 AND ($3 = 0 OR `+lastProgressSQL+` > now() - make_interval(mins => $3)
			OR (NOT EXISTS (SELECT 1 FROM peerdb_stats.flow_errors e WHERE e.flow_name = f.name AND `+stallingErrorSQL(4)+`
				AND e.error_timestamp > now() - `+retryGapSQL(3)+`) AND NOT `+pendingBatchesSQL+`))
		RETURNING f.name`,
		protos.FlowStatus_STATUS_RUNNING, protos.FlowStatus_STATUS_STALLED, int32(thresholdMinutes), stallingClasses)
	if err != nil {
		return nil, fmt.Errorf("failed to clear stalled flows: %w", err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("failed to clear stalled flows: %w", err)
	}
	return names, nil
}
