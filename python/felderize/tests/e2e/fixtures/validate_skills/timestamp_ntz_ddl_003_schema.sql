-- rule: timestamp_ntz_ddl
-- spark: TIMESTAMP_NTZ type in CREATE TABLE DDL — Spark's timezone-naive timestamp type
-- feldera: TIMESTAMP — Feldera has only one timestamp type; drop the _NTZ suffix
CREATE TABLE task_schedule_t3 (
  task_id INT,
  task_name STRING,
  scheduled_at TIMESTAMP_NTZ,
  completed_at TIMESTAMP_NTZ,
  status STRING
);
