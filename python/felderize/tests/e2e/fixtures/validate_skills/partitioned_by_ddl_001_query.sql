-- rule: partitioned_by_ddl
-- spark: CREATE TABLE t (id INT, dt DATE) USING parquet PARTITIONED BY (dt) — Spark table with partitioning clause
-- feldera: Remove USING and PARTITIONED BY clauses: CREATE TABLE t (id INT, dt DATE) — Feldera does not support storage directives
CREATE OR REPLACE TEMP VIEW sales_summary_v1 AS SELECT event_id, sale_amount, event_date FROM sales_events WHERE sale_amount > 100.0;
