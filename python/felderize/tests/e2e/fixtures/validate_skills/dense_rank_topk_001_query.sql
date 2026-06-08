-- rule: dense_rank_topk
-- spark: DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...) in TopK pattern — must be in subquery with WHERE filter
-- feldera: DENSE_RANK() same; wrap in subquery with WHERE dr <= N filter
CREATE OR REPLACE TEMP VIEW top_products_v1 AS
SELECT region, product, revenue, quarter
FROM (
  SELECT region, product, revenue, quarter,
         DENSE_RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS dr
  FROM sales_records
)
WHERE dr <= 2;
