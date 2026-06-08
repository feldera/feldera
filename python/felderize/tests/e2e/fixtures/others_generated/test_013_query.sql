CREATE OR REPLACE TEMP VIEW bm16_customers_without_orders AS
SELECT
  c.customer_id,
  c.customer_name,
  c.country
FROM customer_master c
LEFT ANTI JOIN recent_orders o
  ON c.customer_id = o.customer_id
  AND o.order_date >= DATE '2026-01-01';
