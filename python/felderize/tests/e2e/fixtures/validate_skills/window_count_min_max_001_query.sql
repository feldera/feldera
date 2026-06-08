-- rule: window_count_min_max
-- spark: COUNT(*) OVER (PARTITION BY ... ORDER BY ...) / MIN(col) OVER (...) / MAX(col) OVER (...) — aggregate window functions
-- feldera: COUNT(*) OVER (...) / MIN(col) OVER (...) / MAX(col) OVER (...) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW sales_window_v1 AS SELECT
  transaction_id,
  region,
  amount,
  COUNT(*) OVER (PARTITION BY region ORDER BY sale_date) AS running_count,
  MIN(amount) OVER (PARTITION BY region) AS region_min_amount,
  MAX(amount) OVER (PARTITION BY region ORDER BY sale_date) AS region_max_to_date
FROM sales_data
ORDER BY region, sale_date;
