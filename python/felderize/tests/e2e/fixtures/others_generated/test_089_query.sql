CREATE OR REPLACE TEMP VIEW jn05_retail_left_semi_join AS
SELECT
  c.customer_id,
  c.region,
  c.tier
FROM retail_customers c
LEFT SEMI JOIN (
  SELECT o.customer_id
  FROM retail_orders o
  JOIN retail_payments p
    ON o.order_id = p.order_id
  JOIN retail_stores s
    ON o.store_id = s.store_id
  WHERE p.payment_amount >= 100 AND s.format = 'MALL'
) qualified
  ON c.customer_id = qualified.customer_id;
