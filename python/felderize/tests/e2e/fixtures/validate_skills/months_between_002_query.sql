-- rule: months_between
-- spark: months_between(end_ts, start_ts) — months between two timestamps
-- feldera: DATEDIFF(MONTH, start_ts, end_ts) — integer result; Spark returns fractional months
CREATE OR REPLACE TEMP VIEW emp_service_v2 AS SELECT emp_id, months_between(review_date, hire_date) AS tenure_months FROM employee_tenure WHERE months_between(review_date, hire_date) > 0;
