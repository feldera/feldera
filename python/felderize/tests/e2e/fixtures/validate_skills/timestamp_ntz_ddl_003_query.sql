-- rule: timestamp_ntz_ddl
-- spark: TIMESTAMP_NTZ type in CREATE TABLE DDL — Spark's timezone-naive timestamp type
-- feldera: TIMESTAMP — Feldera has only one timestamp type; drop the _NTZ suffix
CREATE OR REPLACE TEMP VIEW task_view_3 AS SELECT task_id, task_name, scheduled_at, completed_at, status FROM task_schedule_t3 WHERE status = 'completed' ORDER BY completed_at;
