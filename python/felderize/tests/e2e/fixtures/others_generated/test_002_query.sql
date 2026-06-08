CREATE OR REPLACE TEMP VIEW bm02_brand_sales AS
SELECT
  s.region,
  p.category,
  p.brand,
  SUM(f.quantity) AS units_sold,
  SUM(f.net_amount) AS net_revenue
FROM fact_sales f
JOIN dim_product p
  ON f.product_id = p.product_id
JOIN dim_store s
  ON f.store_id = s.store_id
WHERE s.format = 'MALL'
GROUP BY s.region, p.category, p.brand;
