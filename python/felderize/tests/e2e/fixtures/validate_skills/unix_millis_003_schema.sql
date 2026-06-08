-- rule: unix_millis
-- spark: unix_millis(ts) — milliseconds since epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000 AS BIGINT)
CREATE TABLE transaction_log_v3 (txn_id BIGINT, txn_timestamp TIMESTAMP, amount DECIMAL(10,2));
