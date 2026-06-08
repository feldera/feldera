-- rule: cast_interval_literal
-- spark: CAST('3' AS INTERVAL DAY) / CAST('interval 3 day' AS interval day) — string to interval
-- feldera: INTERVAL '3' DAY — drop CAST, use interval literal directly
CREATE TABLE subscription_records (sub_id INT, customer_name STRING, signup_date TIMESTAMP, plan_type STRING); CREATE TABLE renewal_notices (notice_id INT, sub_id INT, notice_date TIMESTAMP, days_until_renewal INT);
