CREATE OR REPLACE TEMP VIEW jn03_retail_using_clause_join AS
SELECT
  store_id,
  region,
  COUNT(DISTINCT order_id) AS order_count,
  SUM(payment_amount) AS paid_amount
FROM retail_orders
JOIN retail_payments USING (order_id)
JOIN retail_stores USING (store_id)
GROUP BY store_id, region;
