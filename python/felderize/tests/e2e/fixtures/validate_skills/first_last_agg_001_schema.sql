-- rule: first_last_agg
-- spark: first(col) / last(col) — return first or last value in a group
-- feldera: MAX(col) — Feldera has no first()/last() aggregate; use MAX(col) as an approximation
CREATE TABLE sales_log (
  sale_id INT,
  region STRING,
  amount INT,
  order_date DATE
);
