-- rule: decode_func
-- spark: decode(expr, s1, r1, s2, r2, ..., default) — Oracle-style conditional matching; NULL-safe equality (NULL = NULL is TRUE) when search values are literals
-- feldera: CASE WHEN expr = s1 THEN r1 WHEN expr = s2 THEN r2 ... ELSE default END — safe rewrite when all search values are non-NULL literals; NULL-safe matching only matters when a search value could be NULL
CREATE OR REPLACE TEMP VIEW status_view AS SELECT
  id,
  code,
  amount,
  decode(code, 'A', 'Active', 'I', 'Inactive', 'P', 'Pending', 'Unknown') AS status_name
FROM status_codes;
