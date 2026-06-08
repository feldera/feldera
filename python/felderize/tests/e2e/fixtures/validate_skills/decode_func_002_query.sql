-- rule: decode_func
-- spark: decode(expr, s1, r1, s2, r2, ..., default) — Oracle-style conditional matching; NULL-safe equality (NULL = NULL is TRUE) when search values are literals
-- feldera: CASE WHEN expr = s1 THEN r1 WHEN expr = s2 THEN r2 ... ELSE default END — safe rewrite when all search values are non-NULL literals; NULL-safe matching only matters when a search value could be NULL
CREATE OR REPLACE TEMP VIEW priority_view AS SELECT
  order_id,
  priority,
  region,
  decode(priority, 1, 'Critical', 2, 'High', 3, 'Medium', 4, 'Low', 0) AS urgency
FROM order_priorities;
