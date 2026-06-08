CREATE OR REPLACE TEMP VIEW bm07_shipping_performance AS
SELECT
  warehouse_id,
  date_trunc('WEEK', shipped_at) AS ship_week,
  AVG(CASE WHEN delivered_at > expected_at THEN 1.0 ELSE 0.0 END) AS late_rate,
  MAX(datediff(delivered_at, shipped_at)) AS max_days_in_transit
FROM shipments
GROUP BY warehouse_id, date_trunc('WEEK', shipped_at);
