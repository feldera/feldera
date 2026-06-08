-- rule: qualify_clause
-- spark: SELECT * FROM (SELECT ..., RANK() OVER (...) AS rnk FROM t) sub WHERE sub.rnk = 1 — standard Spark: filter on window result via subquery (QUALIFY is not in standard Spark SQL)
-- feldera: SELECT ..., RANK() OVER (...) AS rnk FROM t QUALIFY rnk = 1 — QUALIFY is supported directly in Feldera
CREATE OR REPLACE TEMP VIEW top_sales_v1 AS SELECT * FROM (SELECT sale_id, product_name, amount, region, RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS rnk FROM sales_log) sub WHERE sub.rnk = 1;
