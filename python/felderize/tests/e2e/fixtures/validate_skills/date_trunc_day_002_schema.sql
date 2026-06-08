-- rule: date_trunc_day
-- spark: date_trunc('DAY', ts) — truncate timestamp to day
-- feldera: TIMESTAMP_TRUNC(ts, DAY)
CREATE TABLE transaction_data_2 (txn_id INT, txn_timestamp TIMESTAMP, amount DECIMAL(10,2));
