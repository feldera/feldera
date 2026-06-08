-- rule: partitioned_by_ddl
-- spark: CREATE TABLE t (id INT, dt DATE) USING parquet PARTITIONED BY (dt) — Spark table with partitioning clause
-- feldera: Remove USING and PARTITIONED BY clauses: CREATE TABLE t (id INT, dt DATE) — Feldera does not support storage directives
CREATE TABLE inventory_snapshot (product_id INT, stock_count INT, unit_price DECIMAL(10,2), snapshot_date DATE) USING parquet PARTITIONED BY (snapshot_date);
