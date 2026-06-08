-- rule: last_day
-- spark: last_day(d) — last day of the month
-- feldera: DATE_TRUNC(d, MONTH) + INTERVAL '1' MONTH - INTERVAL '1' DAY
CREATE TABLE billing_records (
  record_id INT,
  billing_date DATE,
  service_name STRING
);
