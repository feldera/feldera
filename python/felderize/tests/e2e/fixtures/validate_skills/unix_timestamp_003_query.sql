-- rule: unix_timestamp
-- spark: unix_timestamp(ts) — seconds since epoch
-- feldera: EXTRACT(EPOCH FROM ts)
CREATE OR REPLACE TEMP VIEW unix_ts_v3 AS SELECT txn_id, txn_type, txn_time, amount, unix_timestamp(txn_time) AS unix_seconds FROM transaction_log_003 WHERE amount > 0;
