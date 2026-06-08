CREATE OR REPLACE TEMP VIEW jn02_retail_left_join_after_inner AS
SELECT
  c.customer_id,
  c.tier,
  s.format,
  COALESCE(SUM(p.payment_amount), 0) AS total_paid
FROM retail_customers c
JOIN retail_orders o
  ON c.customer_id = o.customer_id
LEFT JOIN retail_payments p
  ON o.order_id = p.order_id
LEFT JOIN retail_stores s
  ON c.store_id = s.store_id
GROUP BY c.customer_id, c.tier, s.format;
