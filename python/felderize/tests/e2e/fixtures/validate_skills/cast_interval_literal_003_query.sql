-- rule: cast_interval_literal
-- spark: CAST('3' AS INTERVAL DAY) / CAST('interval 3 day' AS interval day) — string to interval
-- feldera: INTERVAL '3' DAY — drop CAST, use interval literal directly
CREATE OR REPLACE TEMP VIEW subscription_timeline AS SELECT subscription_records.sub_id, subscription_records.customer_name, subscription_records.signup_date, (subscription_records.signup_date + CAST('30' AS INTERVAL DAY)) AS first_renewal, (subscription_records.signup_date + CAST('1' AS INTERVAL DAY)) AS warning_date FROM subscription_records WHERE subscription_records.plan_type = 'premium';
