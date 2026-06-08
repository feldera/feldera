CREATE OR REPLACE TEMP VIEW bm35_ranked_daily_regions AS
WITH daily_totals AS (
  SELECT region, sale_date, SUM(amount) AS total_amount FROM regional_revenue GROUP BY region, sale_date
)
SELECT region, sale_date, total_amount,
  RANK() OVER (PARTITION BY sale_date ORDER BY total_amount DESC) AS region_rank
FROM daily_totals;
