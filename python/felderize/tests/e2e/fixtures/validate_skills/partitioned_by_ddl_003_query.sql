-- rule: partitioned_by_ddl
-- spark: CREATE TABLE t (id INT, dt DATE) USING parquet PARTITIONED BY (dt) — Spark table with partitioning clause
-- feldera: Remove USING and PARTITIONED BY clauses: CREATE TABLE t (id INT, dt DATE) — Feldera does not support storage directives
CREATE OR REPLACE TEMP VIEW low_stock_items_v3 AS SELECT product_id, stock_count, unit_price, snapshot_date FROM inventory_snapshot WHERE stock_count < 50;
