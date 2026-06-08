-- rule: months_between
-- spark: months_between(end_ts, start_ts) — months between two timestamps
-- feldera: DATEDIFF(MONTH, start_ts, end_ts) — integer result; Spark returns fractional months
CREATE TABLE project_timeline (project_id INT, start_date TIMESTAMP, end_date TIMESTAMP);
