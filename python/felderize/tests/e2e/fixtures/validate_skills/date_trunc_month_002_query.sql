-- rule: date_trunc_month
-- spark: date_trunc('MONTH', ts) — truncate timestamp to month
-- feldera: TIMESTAMP_TRUNC(ts, MONTH)  or  DATE_TRUNC(d, MONTH) for DATE input
CREATE OR REPLACE TEMP VIEW monthly_transactions AS SELECT
  date_trunc('MONTH', txn_timestamp) AS transaction_month,
  COUNT(*) AS txn_count,
  SUM(amount) AS total_amount
FROM transactions
GROUP BY date_trunc('MONTH', txn_timestamp)
ORDER BY transaction_month;
