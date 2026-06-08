-- rule: months_between
-- spark: months_between(end_ts, start_ts) — months between two timestamps
-- feldera: DATEDIFF(MONTH, start_ts, end_ts) — integer result; Spark returns fractional months
CREATE OR REPLACE TEMP VIEW subscription_duration_v3 AS SELECT sub_id, ROUND(months_between(cancellation_ts, activation_ts), 2) AS duration_months FROM subscription_audit;
