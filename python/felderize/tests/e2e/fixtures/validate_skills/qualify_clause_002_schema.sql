-- rule: qualify_clause
-- spark: SELECT * FROM (SELECT ..., RANK() OVER (...) AS rnk FROM t) sub WHERE sub.rnk = 1 — standard Spark: filter on window result via subquery (QUALIFY is not in standard Spark SQL)
-- feldera: SELECT ..., RANK() OVER (...) AS rnk FROM t QUALIFY rnk = 1 — QUALIFY is supported directly in Feldera
CREATE TABLE employee_performance (
  emp_id INT,
  emp_name STRING,
  dept STRING,
  salary DECIMAL(10, 2),
  rating INT
);
