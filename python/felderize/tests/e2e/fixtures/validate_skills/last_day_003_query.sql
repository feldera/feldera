-- rule: last_day
-- spark: last_day(d) — last day of the month
-- feldera: DATE_TRUNC(d, MONTH) + INTERVAL '1' MONTH - INTERVAL '1' DAY
CREATE OR REPLACE TEMP VIEW billing_month_end AS SELECT
  record_id,
  billing_date,
  last_day(billing_date) AS due_date,
  service_name
FROM billing_records;
