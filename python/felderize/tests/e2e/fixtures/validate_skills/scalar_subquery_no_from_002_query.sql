-- rule: scalar_subquery_no_from
-- spark: HAVING (SELECT expr) or WHERE (SELECT expr) — scalar subquery with no FROM clause used as shorthand for the expression
-- feldera: Rewrite: drop the SELECT wrapper → HAVING expr or WHERE expr. If subquery has FROM, leave as-is.
CREATE OR REPLACE TEMP VIEW high_earners_v2 AS SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_sal FROM employee_records_v2 WHERE salary > (SELECT 50000.0) GROUP BY department;
