-- rule: first_last_agg
-- spark: first(col) / last(col) — return first or last value in a group
-- feldera: MAX(col) — Feldera has no first()/last() aggregate; use MAX(col) as an approximation
CREATE TABLE product_prices (
  prod_id INT,
  category STRING,
  price INT,
  week_num INT
);
