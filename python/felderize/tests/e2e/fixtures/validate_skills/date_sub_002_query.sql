-- rule: date_sub
-- spark: date_sub(date, n) — subtract n days from a date
-- feldera: date - INTERVAL 'n' DAY  (literal n) or  date - n * INTERVAL '1' DAY  (column n)
CREATE OR REPLACE TEMP VIEW transaction_results_v2 AS SELECT txn_id, date_sub(txn_date, subtract_days) AS adjusted_date FROM transaction_dates;
