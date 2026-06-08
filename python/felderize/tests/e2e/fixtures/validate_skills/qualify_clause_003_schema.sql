-- rule: qualify_clause
-- spark: SELECT * FROM (SELECT ..., RANK() OVER (...) AS rnk FROM t) sub WHERE sub.rnk = 1 — standard Spark: filter on window result via subquery (QUALIFY is not in standard Spark SQL)
-- feldera: SELECT ..., RANK() OVER (...) AS rnk FROM t QUALIFY rnk = 1 — QUALIFY is supported directly in Feldera
CREATE TABLE order_details (
  order_id INT,
  customer_id INT,
  order_value DOUBLE,
  order_status STRING,
  order_timestamp TIMESTAMP
);
