-- rule: cube_agg
-- spark: CUBE(a, b) — all combinations of grouping
-- feldera: CUBE(a, b) — same
CREATE TABLE sales_data (
  region STRING,
  product STRING,
  amount DECIMAL(10,2)
);
