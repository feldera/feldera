-- rule: months_between
-- spark: months_between(end_ts, start_ts) — months between two timestamps
-- feldera: DATEDIFF(MONTH, start_ts, end_ts) — integer result; Spark returns fractional months
CREATE OR REPLACE TEMP VIEW project_months_v1 AS SELECT project_id, months_between(end_date, start_date) AS months_elapsed FROM project_timeline;
