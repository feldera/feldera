-- rule: count_sum_min_max_agg
-- spark: COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) — basic aggregate functions
-- feldera: COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW sales_summary AS SELECT
  COUNT(*) AS total_transactions,
  COUNT(amount) AS non_null_amounts,
  SUM(quantity) AS total_quantity,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount
FROM sales_metrics;
