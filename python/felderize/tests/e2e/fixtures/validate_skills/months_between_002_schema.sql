-- rule: months_between
-- spark: months_between(end_ts, start_ts) — months between two timestamps
-- feldera: DATEDIFF(MONTH, start_ts, end_ts) — integer result; Spark returns fractional months
CREATE TABLE employee_tenure (emp_id INT, hire_date TIMESTAMP, review_date TIMESTAMP);
