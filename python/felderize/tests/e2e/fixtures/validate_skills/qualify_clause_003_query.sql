-- rule: qualify_clause
-- spark: SELECT * FROM (SELECT ..., RANK() OVER (...) AS rnk FROM t) sub WHERE sub.rnk = 1 — standard Spark: filter on window result via subquery (QUALIFY is not in standard Spark SQL)
-- feldera: SELECT ..., RANK() OVER (...) AS rnk FROM t QUALIFY rnk = 1 — QUALIFY is supported directly in Feldera
CREATE OR REPLACE TEMP VIEW latest_order_v3 AS SELECT * FROM (SELECT order_id, customer_id, order_value, order_status, order_timestamp, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_timestamp DESC) AS rnk FROM order_details) sub WHERE sub.rnk = 1;
