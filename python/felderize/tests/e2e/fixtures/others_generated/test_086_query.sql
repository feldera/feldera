CREATE OR REPLACE TEMP VIEW jn01_retail_inner_join_chain AS
SELECT
  c.region,
  s.format,
  p.payment_method,
  COUNT(*) AS order_count,
  SUM(p.payment_amount) AS paid_amount
FROM retail_customers c
JOIN retail_orders o
  ON c.customer_id = o.customer_id
JOIN retail_payments p
  ON o.order_id = p.order_id
JOIN retail_stores s
  ON o.store_id = s.store_id
GROUP BY c.region, s.format, p.payment_method;
