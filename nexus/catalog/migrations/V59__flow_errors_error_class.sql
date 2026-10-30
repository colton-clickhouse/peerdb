ALTER TABLE peerdb_stats.flow_errors ADD COLUMN IF NOT EXISTS error_class TEXT;
ALTER TABLE peerdb_stats.flow_errors ADD COLUMN IF NOT EXISTS error_action TEXT;
CREATE INDEX IF NOT EXISTS idx_flow_errors_flow_name_error_timestamp ON peerdb_stats.flow_errors (flow_name, error_timestamp);
