CREATE OR REPLACE TEMP VIEW revenue_by_region_month AS
SELECT
  region,
  date_trunc('MONTH', created_at) AS order_month,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM orders
WHERE status IN ('PAID', 'SHIPPED')
GROUP BY region, date_trunc('MONTH', created_at);
