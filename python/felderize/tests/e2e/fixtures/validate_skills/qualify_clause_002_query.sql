-- rule: qualify_clause
-- spark: SELECT * FROM (SELECT ..., RANK() OVER (...) AS rnk FROM t) sub WHERE sub.rnk = 1 — standard Spark: filter on window result via subquery (QUALIFY is not in standard Spark SQL)
-- feldera: SELECT ..., RANK() OVER (...) AS rnk FROM t QUALIFY rnk = 1 — QUALIFY is supported directly in Feldera
CREATE OR REPLACE TEMP VIEW highest_rated_v2 AS SELECT * FROM (SELECT emp_id, emp_name, dept, salary, rating, RANK() OVER (PARTITION BY dept ORDER BY rating DESC) AS rnk FROM employee_performance) sub WHERE sub.rnk = 1;
