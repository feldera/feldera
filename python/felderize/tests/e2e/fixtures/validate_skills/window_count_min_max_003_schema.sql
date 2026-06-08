-- rule: window_count_min_max
-- spark: COUNT(*) OVER (PARTITION BY ... ORDER BY ...) / MIN(col) OVER (...) / MAX(col) OVER (...) — aggregate window functions
-- feldera: COUNT(*) OVER (...) / MIN(col) OVER (...) / MAX(col) OVER (...) — all work identically in Feldera, no translation needed
CREATE TABLE customer_orders (
  order_id BIGINT,
  customer_id INT,
  order_value INT,
  order_timestamp TIMESTAMP
);
