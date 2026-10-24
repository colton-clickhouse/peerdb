-- +goose Up
ALTER TABLE peerdb_stats.flow_errors ADD COLUMN IF NOT EXISTS error_class TEXT;
ALTER TABLE peerdb_stats.flow_errors ADD COLUMN IF NOT EXISTS error_action TEXT;
CREATE INDEX IF NOT EXISTS idx_cdc_batches_flow_name_end_time ON peerdb_stats.cdc_batches (flow_name, end_time);
