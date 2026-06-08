-- rule: add_months
-- spark: add_months(date, n) — add n months to a date
-- feldera: date + INTERVAL 'n' MONTH  (literal n) or  date + n * INTERVAL '1' MONTH  (column n)
CREATE TABLE subscription_plan (subscriber_id INT, signup_date DATE, trial_period_months INT);
