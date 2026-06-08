-- rule: count_sum_min_max_agg
-- spark: COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) — basic aggregate functions
-- feldera: COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) — all work identically in Feldera, no translation needed
CREATE TABLE product_inventory (
  product_id BIGINT,
  stock_level INT,
  price DECIMAL(10,2),
  last_restock DATE
);
