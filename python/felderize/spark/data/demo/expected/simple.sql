CREATE TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  region VARCHAR,
  amount DECIMAL(12,2),
  status VARCHAR,
  created_at TIMESTAMP
);

CREATE VIEW revenue_by_region_month AS
SELECT
  region,
  TIMESTAMP_TRUNC(created_at, MONTH) AS order_month,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM orders
WHERE status IN ('PAID', 'SHIPPED')
GROUP BY region, TIMESTAMP_TRUNC(created_at, MONTH);
