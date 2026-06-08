CREATE OR REPLACE TEMP VIEW val68_latest_order_per_customer AS
WITH ranked_orders AS (
  SELECT
    customer_id,
    order_id,
    order_ts,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC, order_id DESC) AS rn
  FROM order_fact
)
SELECT customer_id, order_id, order_ts
FROM ranked_orders
WHERE rn = 1;
