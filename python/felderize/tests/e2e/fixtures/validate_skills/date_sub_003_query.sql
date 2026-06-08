-- rule: date_sub
-- spark: date_sub(date, n) — subtract n days from a date
-- feldera: date - INTERVAL 'n' DAY  (literal n) or  date - n * INTERVAL '1' DAY  (column n)
CREATE OR REPLACE TEMP VIEW schedule_prior_v3 AS SELECT schedule_id, date_sub(base_date, lookback_days) AS prior_date FROM schedule_dates;
