-- rule: unix_millis
-- spark: unix_millis(ts) — milliseconds since epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000 AS BIGINT)
CREATE OR REPLACE TEMP VIEW transaction_millis_v3 AS SELECT txn_id, amount, unix_millis(txn_timestamp) AS millis_since_epoch FROM transaction_log_v3;
