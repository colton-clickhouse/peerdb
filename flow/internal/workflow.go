package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

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
	if _, err := pool.Exec(ctx, "UPDATE flows SET status=$1,updated_at=now() WHERE workflow_id=$2", status, workflowID); err != nil {
		slog.ErrorContext(ctx, "failed to update flow status", slog.Any("error", err), slog.String("flowID", workflowID))
		return status, fmt.Errorf("failed to update flow status: %w", err)
	}
	return status, nil
}

func UpdateFlowStatusWithNameInCatalog(ctx context.Context, pool shared.CatalogPool,
	flowName string, status protos.FlowStatus,
) (protos.FlowStatus, error) {
	if _, err := pool.Exec(ctx, "UPDATE flows SET status=$1,updated_at=now() WHERE name=$2", status, flowName); err != nil {
		slog.ErrorContext(ctx, "failed to update flow status", slog.Any("error", err), slog.String("flowName", flowName))
		return status, fmt.Errorf("failed to update flow status: %w", err)
	}
	return status, nil
}

// lastProgressSQL is when the flow f last made progress: its newest normalized CDC batch (every destination sets
// cdc_batches.end_time once a batch is normalized) or finished QRep partition, its own or its initial snapshot's
// (which runs under clone flows, found through qrep_runs.parent_mirror_name as the mirror status does), or its
// creation if none. Only the current mirror counts: a dropped mirror can leave batches behind, which one recreated
// under the same name mustn't inherit.
const lastProgressSQL = `coalesce(greatest(
	(SELECT max(b.end_time) FROM peerdb_stats.cdc_batches b WHERE b.flow_name = f.name AND b.sync_time >= f.created_at),
	(SELECT max(p.end_time) FROM peerdb_stats.qrep_runs r JOIN peerdb_stats.qrep_partitions p ON p.flow_name = r.flow_name
		WHERE (r.flow_name = f.name OR r.parent_mirror_name = f.name) AND p.start_time >= f.created_at)
), f.created_at)`

// FlowLastProgress returns when the flow last made progress (lastProgressSQL)
func FlowLastProgress(ctx context.Context, pool shared.CatalogPool, flowName string) (time.Time, error) {
	var t time.Time
	if err := pool.QueryRow(ctx, `SELECT `+lastProgressSQL+` FROM flows f WHERE f.name = $1`, flowName).Scan(&t); err != nil {
		return time.Time{}, fmt.Errorf("failed to read the last progress of flow %s: %w", flowName, err)
	}
	return t, nil
}

// FlowLastError returns the flow's newest error log, if it has one
func FlowLastError(ctx context.Context, pool shared.CatalogPool, flowName string) (string, time.Time, bool, error) {
	var message string
	var at time.Time
	if err := pool.QueryRow(ctx, `SELECT error_message, error_timestamp FROM peerdb_stats.flow_errors
		WHERE flow_name = $1 AND error_type = 'error' ORDER BY error_timestamp DESC LIMIT 1`, flowName,
	).Scan(&message, &at); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", time.Time{}, false, nil
		}
		return "", time.Time{}, false, fmt.Errorf("failed to read the last error of flow %s: %w", flowName, err)
	}
	return message, at, true, nil
}
