-- rule: partitioned_by_ddl
-- spark: CREATE TABLE t (id INT, dt DATE) USING parquet PARTITIONED BY (dt) — Spark table with partitioning clause
-- feldera: Remove USING and PARTITIONED BY clauses: CREATE TABLE t (id INT, dt DATE) — Feldera does not support storage directives
CREATE TABLE sales_events (event_id INT, sale_amount DOUBLE, event_date DATE) USING parquet PARTITIONED BY (event_date);
