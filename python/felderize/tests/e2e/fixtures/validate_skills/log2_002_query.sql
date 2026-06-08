-- rule: log2
-- spark: log2(x) — base-2 logarithm
-- feldera: LOG(x, 2)
CREATE OR REPLACE TEMP VIEW bandwidth_log_v2 AS SELECT channel_id, bandwidth_mbps, log2(bandwidth_mbps) AS log2_bandwidth FROM bandwidth_values WHERE bandwidth_mbps > 0.5;
