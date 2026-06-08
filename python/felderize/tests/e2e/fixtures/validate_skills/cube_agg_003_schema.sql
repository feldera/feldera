-- rule: cube_agg
-- spark: CUBE(a, b) — all combinations of grouping
-- feldera: CUBE(a, b) — same
CREATE TABLE transaction_log (
  customer_type STRING,
  payment_method STRING,
  transaction_amount DECIMAL(10,2)
);
