CREATE OR REPLACE TEMP VIEW bm24_top_regions_by_large_orders AS
SELECT
  region,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM regional_orders
GROUP BY region
HAVING SUM(amount) >= 10000
ORDER BY total_amount DESC, region ASC
LIMIT 10;
