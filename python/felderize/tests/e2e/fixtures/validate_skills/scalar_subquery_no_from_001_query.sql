-- rule: scalar_subquery_no_from
-- spark: HAVING (SELECT expr) or WHERE (SELECT expr) — scalar subquery with no FROM clause used as shorthand for the expression
-- feldera: Rewrite: drop the SELECT wrapper → HAVING expr or WHERE expr. If subquery has FROM, leave as-is.
CREATE OR REPLACE TEMP VIEW sales_summary_v1 AS SELECT region, COUNT(*) as cnt, SUM(amount) as total FROM sales_log_v1 GROUP BY region HAVING SUM(amount) > (SELECT 500.0);
