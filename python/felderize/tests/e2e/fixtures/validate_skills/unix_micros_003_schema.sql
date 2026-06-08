-- rule: unix_micros
-- spark: unix_micros(ts) — microseconds since Unix epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000000 AS BIGINT)
CREATE TABLE transaction_log_v3 (txn_id BIGINT, txn_timestamp TIMESTAMP, amount DECIMAL(10,2));
