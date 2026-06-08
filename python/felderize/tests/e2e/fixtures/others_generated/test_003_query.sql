CREATE OR REPLACE TEMP VIEW bm03_customer_refund_summary AS
SELECT
  c.customer_id,
  c.customer_name,
  COALESCE(SUM(r.refund_amount), 0) AS total_refunds,
  CASE
    WHEN MAX(r.created_at) IS NULL THEN 'NO_REFUNDS'
    ELSE 'HAS_REFUNDS'
  END AS refund_status
FROM customers c
LEFT JOIN refunds r
  ON c.customer_id = r.customer_id
GROUP BY c.customer_id, c.customer_name;
