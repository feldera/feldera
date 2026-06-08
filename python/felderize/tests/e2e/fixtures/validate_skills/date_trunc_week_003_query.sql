-- rule: date_trunc_week
-- spark: date_trunc('WEEK', d) / trunc(d, 'WEEK') — truncate date to start of week; Spark uses Monday as first day of week
-- feldera: DATE_TRUNC(d - INTERVAL '1' DAY, WEEK) + INTERVAL '1' DAY — Feldera truncates to Sunday; subtract 1 day before truncating to handle all days correctly
CREATE OR REPLACE TEMP VIEW weekly_transactions_v3 AS SELECT COUNT(*) AS txn_count, SUM(value) AS total_value, date_trunc('WEEK', txn_date) AS week_start FROM transaction_data WHERE status = 'completed' GROUP BY date_trunc('WEEK', txn_date) ORDER BY week_start;
