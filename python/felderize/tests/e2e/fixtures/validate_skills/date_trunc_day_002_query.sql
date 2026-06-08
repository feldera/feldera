-- rule: date_trunc_day
-- spark: date_trunc('DAY', ts) — truncate timestamp to day
-- feldera: TIMESTAMP_TRUNC(ts, DAY)
CREATE OR REPLACE TEMP VIEW daily_totals_v2 AS SELECT date_trunc('DAY', txn_timestamp) AS transaction_day, SUM(amount) AS daily_sum, COUNT(*) AS txn_count FROM transaction_data_2 GROUP BY date_trunc('DAY', txn_timestamp) ORDER BY transaction_day;
