-- rule: date_trunc_year
-- spark: date_trunc('YEAR', ts) — truncate timestamp to year
-- feldera: TIMESTAMP_TRUNC(ts, YEAR)
CREATE TABLE transaction_records (
  txn_id INT,
  amount DECIMAL(10,2),
  txn_timestamp TIMESTAMP
);
