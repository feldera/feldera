-- rule: window_count_min_max
-- spark: COUNT(*) OVER (PARTITION BY ... ORDER BY ...) / MIN(col) OVER (...) / MAX(col) OVER (...) — aggregate window functions
-- feldera: COUNT(*) OVER (...) / MIN(col) OVER (...) / MAX(col) OVER (...) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW order_window_v3 AS SELECT
  order_id,
  customer_id,
  order_value,
  COUNT(*) OVER (PARTITION BY customer_id ORDER BY order_timestamp) AS customer_order_count,
  MIN(order_value) OVER (PARTITION BY customer_id) AS cust_min_order,
  MAX(order_value) OVER (PARTITION BY customer_id ORDER BY order_timestamp) AS cust_max_to_date
FROM customer_orders
ORDER BY customer_id, order_timestamp;
