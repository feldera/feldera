-- rule: unix_micros
-- spark: unix_micros(ts) — microseconds since Unix epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000000 AS BIGINT)
CREATE OR REPLACE TEMP VIEW transaction_micros_v3 AS SELECT txn_id, unix_micros(txn_timestamp) AS micros_since_epoch, amount FROM transaction_log_v3 ORDER BY micros_since_epoch;
