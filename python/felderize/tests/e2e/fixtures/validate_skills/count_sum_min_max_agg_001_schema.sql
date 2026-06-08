-- rule: count_sum_min_max_agg
-- spark: COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) — basic aggregate functions
-- feldera: COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) — all work identically in Feldera, no translation needed
CREATE TABLE sales_metrics (
  transaction_id INT,
  amount DECIMAL(10,2),
  quantity INT,
  status STRING
);
