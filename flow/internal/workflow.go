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
// (which runs under clone flows, whose partitions have the mirror as parent_mirror_name), or its creation if none.
// Only the current mirror counts: a dropped mirror can leave batches behind, which one recreated under the same name
// mustn't inherit.
const lastProgressSQL = `coalesce(greatest(
	(SELECT max(b.end_time) FROM peerdb_stats.cdc_batches b WHERE b.flow_name = f.name AND b.sync_time >= f.created_at),
	(SELECT max(p.end_time) FROM peerdb_stats.qrep_partitions p
		WHERE (p.flow_name = f.name OR p.parent_mirror_name = f.name) AND p.start_time >= f.created_at)
), f.created_at)`

// flowErrorNamesSQL is the flow names the error log of the flow named $1 is under: its own, and its initial
// snapshot's clone flows (QRep runs with the mirror as parent_mirror_name)
const flowErrorNamesSQL = `ARRAY(SELECT $1::text UNION ALL
	SELECT r.flow_name FROM peerdb_stats.qrep_runs r WHERE r.parent_mirror_name = $1)`

type FlowProgress struct {
	// LastProgress is when the flow last made progress (lastProgressSQL)
	LastProgress time.Time
	// Since is how long ago that was, by the catalog's clock
	Since time.Duration
	// FailingWithoutProgress: it has logged errors for longer than the threshold without progress: it has an error newer
	// than its last progress and older than the threshold
	FailingWithoutProgress bool
}

// GetFlowProgress returns when the flow last made progress and whether it has been failing without progress for
// longer than threshold (never for 0)
func GetFlowProgress(ctx context.Context, pool shared.CatalogPool, flowName string, threshold time.Duration,
) (FlowProgress, error) {
	var progress FlowProgress
	var sinceSeconds float64
	if err := pool.QueryRow(ctx, `SELECT p.at, extract(epoch FROM now() - p.at)::float8,
		$2::float8 > 0 AND EXISTS(SELECT 1 FROM peerdb_stats.flow_errors e WHERE e.flow_name = ANY(`+flowErrorNamesSQL+`)
			AND e.error_type = 'error' AND e.error_timestamp > p.at AND e.error_timestamp <= now() - make_interval(secs => $2::float8))
		FROM flows f CROSS JOIN LATERAL (SELECT `+lastProgressSQL+` AS at) p WHERE f.name = $1`,
		flowName, threshold.Seconds(),
	).Scan(&progress.LastProgress, &sinceSeconds, &progress.FailingWithoutProgress); err != nil {
		return FlowProgress{}, fmt.Errorf("failed to read the progress of flow %s: %w", flowName, err)
	}
	progress.Since = time.Duration(sinceSeconds * float64(time.Second))
	return progress, nil
}

// FlowLastError returns the newest error log of the flow or its initial snapshot, if it has one
func FlowLastError(ctx context.Context, pool shared.CatalogPool, flowName string) (string, time.Time, bool, error) {
	var message string
	var at time.Time
	if err := pool.QueryRow(ctx, `SELECT error_message, error_timestamp FROM peerdb_stats.flow_errors
		WHERE flow_name = ANY(`+flowErrorNamesSQL+`) AND error_type = 'error' ORDER BY error_timestamp DESC LIMIT 1`, flowName,
	).Scan(&message, &at); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", time.Time{}, false, nil
		}
		return "", time.Time{}, false, fmt.Errorf("failed to read the last error of flow %s: %w", flowName, err)
	}
	return message, at, true, nil
}
