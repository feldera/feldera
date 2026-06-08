CREATE OR REPLACE TEMP VIEW bm23_store_product_rankings AS
SELECT
  store_id,
  product_id,
  revenue,
  RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) AS revenue_rank,
  DENSE_RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) AS revenue_dense_rank,
  NTILE(4) OVER (PARTITION BY store_id ORDER BY revenue DESC) AS revenue_quartile
FROM store_product_sales;
