CREATE INDEX IF NOT EXISTS idx_cdc_batches_flow_name_end_time ON peerdb_stats.cdc_batches (flow_name, end_time);
