-- rule: date_add
-- spark: date_add(date, n) — add n days to a date
-- feldera: date + INTERVAL 'n' DAY  (literal n) or  date + n * INTERVAL '1' DAY  (column n)
CREATE OR REPLACE TEMP VIEW subscription_expiry_v3 AS SELECT sub_id, signup_date, trial_period_days, date_add(signup_date, trial_period_days) AS expiry_date FROM subscription_info WHERE trial_period_days > 0;
