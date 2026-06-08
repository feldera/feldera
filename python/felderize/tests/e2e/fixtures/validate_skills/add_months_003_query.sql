-- rule: add_months
-- spark: add_months(date, n) — add n months to a date
-- feldera: date + INTERVAL 'n' MONTH  (literal n) or  date + n * INTERVAL '1' MONTH  (column n)
CREATE OR REPLACE TEMP VIEW subscription_plan_v3 AS SELECT subscriber_id, signup_date, add_months(signup_date, trial_period_months) AS trial_end_date, add_months(signup_date, trial_period_months + 12) AS first_renewal_date FROM subscription_plan;
