-- rule: partitioned_by_ddl
-- spark: CREATE TABLE t (id INT, dt DATE) USING parquet PARTITIONED BY (dt) — Spark table with partitioning clause
-- feldera: Remove USING and PARTITIONED BY clauses: CREATE TABLE t (id INT, dt DATE) — Feldera does not support storage directives
CREATE OR REPLACE TEMP VIEW active_users_v2 AS SELECT user_id, action, log_timestamp, log_date FROM user_logs WHERE action IN ('login', 'purchase') ORDER BY log_date DESC;
