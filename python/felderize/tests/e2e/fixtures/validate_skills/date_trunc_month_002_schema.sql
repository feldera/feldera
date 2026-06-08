-- rule: date_trunc_month
-- spark: date_trunc('MONTH', ts) — truncate timestamp to month
-- feldera: TIMESTAMP_TRUNC(ts, MONTH)  or  DATE_TRUNC(d, MONTH) for DATE input
CREATE TABLE transactions (
  txn_id INT,
  amount DECIMAL(10, 2),
  txn_timestamp TIMESTAMP
);
