-- rule: last_day
-- spark: last_day(d) — last day of the month
-- feldera: DATE_TRUNC(d, MONTH) + INTERVAL '1' MONTH - INTERVAL '1' DAY
CREATE OR REPLACE TEMP VIEW transaction_end_of_month AS SELECT
  transaction_id,
  trans_date,
  last_day(trans_date) AS billing_cutoff_date,
  amount
FROM transaction_dates;
