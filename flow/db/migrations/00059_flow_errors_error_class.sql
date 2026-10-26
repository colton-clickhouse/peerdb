-- +goose NO TRANSACTION
-- +goose Up
-- each error's classifier class and action, for the STALLED status (NULL for errors recorded before this)
ALTER TABLE peerdb_stats.flow_errors ADD COLUMN IF NOT EXISTS error_class TEXT;
ALTER TABLE peerdb_stats.flow_errors ADD COLUMN IF NOT EXISTS error_action TEXT;
-- STALLED reads a flow's recent errors
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_flow_errors_flow_name_error_timestamp
    ON peerdb_stats.flow_errors (flow_name, error_timestamp);
