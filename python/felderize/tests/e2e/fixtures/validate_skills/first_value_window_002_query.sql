-- rule: first_value_window
-- spark: FIRST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — first value in window partition
-- feldera: FIRST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — same syntax; ROWS/RANGE frame clauses not supported, partition must be unbounded
CREATE OR REPLACE TEMP VIEW product_first_price_v2 AS SELECT product_id, category, price, FIRST_VALUE(price) OVER (PARTITION BY category ORDER BY price_date) AS first_price_per_category FROM product_price;
