-- rule: scalar_subquery_no_from
-- spark: HAVING (SELECT expr) or WHERE (SELECT expr) — scalar subquery with no FROM clause used as shorthand for the expression
-- feldera: Rewrite: drop the SELECT wrapper → HAVING expr or WHERE expr. If subquery has FROM, leave as-is.
CREATE TABLE sales_log_v1 (transaction_id INT, amount DOUBLE, region STRING);
