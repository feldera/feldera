-- rule: unix_timestamp
-- spark: unix_timestamp(ts) — seconds since epoch
-- feldera: EXTRACT(EPOCH FROM ts)
CREATE TABLE transaction_log_003 (txn_id INT, txn_type STRING, txn_time TIMESTAMP, amount DECIMAL(10,2));
