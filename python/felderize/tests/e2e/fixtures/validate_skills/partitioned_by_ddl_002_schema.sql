-- rule: partitioned_by_ddl
-- spark: CREATE TABLE t (id INT, dt DATE) USING parquet PARTITIONED BY (dt) — Spark table with partitioning clause
-- feldera: Remove USING and PARTITIONED BY clauses: CREATE TABLE t (id INT, dt DATE) — Feldera does not support storage directives
CREATE TABLE user_logs (user_id BIGINT, action STRING, log_timestamp TIMESTAMP, log_date DATE) USING parquet PARTITIONED BY (log_date);
