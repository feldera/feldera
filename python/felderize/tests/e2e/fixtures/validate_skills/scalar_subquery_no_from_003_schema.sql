-- rule: scalar_subquery_no_from
-- spark: HAVING (SELECT expr) or WHERE (SELECT expr) — scalar subquery with no FROM clause used as shorthand for the expression
-- feldera: Rewrite: drop the SELECT wrapper → HAVING expr or WHERE expr. If subquery has FROM, leave as-is.
CREATE TABLE inventory_v3 (product_id INT, quantity INT, price DOUBLE, category STRING);
