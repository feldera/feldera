CREATE OR REPLACE TEMP VIEW bm31_top_sellers AS
SELECT seller_id, sale_date, revenue FROM (
  SELECT seller_id, sale_date, revenue,
    DENSE_RANK() OVER (PARTITION BY sale_date ORDER BY revenue DESC) AS dr
  FROM seller_daily_sales
) ranked WHERE dr <= 5;
