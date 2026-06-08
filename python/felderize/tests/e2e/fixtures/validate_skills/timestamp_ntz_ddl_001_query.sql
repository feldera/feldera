-- rule: timestamp_ntz_ddl
-- spark: TIMESTAMP_NTZ type in CREATE TABLE DDL — Spark's timezone-naive timestamp type
-- feldera: TIMESTAMP — Feldera has only one timestamp type; drop the _NTZ suffix
CREATE OR REPLACE TEMP VIEW event_view_1 AS SELECT event_id, event_name, event_time, duration_ms FROM event_log_t1 WHERE duration_ms > 0 ORDER BY event_time;
