-- rule: zeroifnull
-- spark: ZEROIFNULL(x) — return 0 if x is NULL
-- feldera: COALESCE(x, 0)
CREATE TABLE sales_metrics (
  transaction_id INT,
  amount DECIMAL(10,2),
  discount DECIMAL(10,2)
);
