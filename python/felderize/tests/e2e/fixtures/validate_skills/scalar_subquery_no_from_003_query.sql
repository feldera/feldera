-- rule: scalar_subquery_no_from
-- spark: HAVING (SELECT expr) or WHERE (SELECT expr) — scalar subquery with no FROM clause used as shorthand for the expression
-- feldera: Rewrite: drop the SELECT wrapper → HAVING expr or WHERE expr. If subquery has FROM, leave as-is.
CREATE OR REPLACE TEMP VIEW low_stock_v3 AS SELECT category, product_id, quantity, price FROM inventory_v3 WHERE quantity < (SELECT 30) AND price > (SELECT 10.0);
