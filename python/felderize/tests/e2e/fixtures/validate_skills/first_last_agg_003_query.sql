-- rule: first_last_agg
-- spark: first(col) / last(col) — return first or last value in a group
-- feldera: MAX(col) — Feldera has no first()/last() aggregate; use MAX(col) as an approximation
CREATE OR REPLACE TEMP VIEW category_price_bounds AS
SELECT
  category,
  first(price) AS week_one_price,
  last(price) AS latest_price,
  MAX(price) AS max_price,
  MIN(price) AS min_price
FROM product_prices
GROUP BY category;
