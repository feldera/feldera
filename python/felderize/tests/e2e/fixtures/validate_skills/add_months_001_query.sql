-- rule: add_months
-- spark: add_months(date, n) — add n months to a date
-- feldera: date + INTERVAL 'n' MONTH  (literal n) or  date + n * INTERVAL '1' MONTH  (column n)
CREATE OR REPLACE TEMP VIEW project_timeline_v1 AS SELECT project_id, start_date, add_months(start_date, months_to_add) AS target_date FROM project_timeline;
