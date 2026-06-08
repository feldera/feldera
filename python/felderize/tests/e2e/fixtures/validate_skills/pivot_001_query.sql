-- rule: pivot
-- spark: PIVOT(COUNT(col) FOR x IN ('A','B','C')) — pivot rows to columns
-- feldera: NULLIF(COUNT(CASE WHEN x = 'A' THEN col END), 0) AS A, ... — NULLIF wraps COUNT to match Spark NULL semantics (Spark returns NULL for empty buckets, COUNT returns 0)
CREATE OR REPLACE TEMP VIEW sales_pivot_v1 AS
SELECT *
FROM sales_data
PIVOT (
  COUNT(amount) FOR category IN ('Electronics', 'Clothing', 'Books')
);
