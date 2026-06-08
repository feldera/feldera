-- rule: date_add
-- spark: date_add(date, n) — add n days to a date
-- feldera: date + INTERVAL 'n' DAY  (literal n) or  date + n * INTERVAL '1' DAY  (column n)
CREATE TABLE subscription_info (sub_id INT, signup_date DATE, trial_period_days INT); CREATE TABLE renewal_data (renewal_id INT, renewal_date DATE);
