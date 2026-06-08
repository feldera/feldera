-- rule: nvl
-- spark: NVL(a, b) — return b if a is NULL
-- feldera: COALESCE(a, b) — NVL not supported in Feldera
CREATE TABLE order_transactions (
  order_id INT,
  customer_name STRING,
  discount_percent DECIMAL(5,2),
  notes STRING
);
