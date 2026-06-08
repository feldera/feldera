-- rule: count_sum_min_max_agg
-- spark: COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) — basic aggregate functions
-- feldera: COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW inventory_stats AS SELECT
  COUNT(*) AS total_products,
  COUNT(stock_level) AS items_in_stock,
  SUM(stock_level) AS total_units,
  MIN(stock_level) AS lowest_stock,
  MAX(stock_level) AS highest_stock,
  MIN(price) AS cheapest_item,
  MAX(price) AS most_expensive_item
FROM product_inventory;
