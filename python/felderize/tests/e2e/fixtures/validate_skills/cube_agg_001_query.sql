-- rule: cube_agg
-- spark: CUBE(a, b) — all combinations of grouping
-- feldera: CUBE(a, b) — same
CREATE OR REPLACE TEMP VIEW cube_agg_results_001 AS
SELECT
  region,
  product,
  SUM(amount) as total_sales
FROM sales_data
GROUP BY CUBE(region, product)
ORDER BY region, product;
