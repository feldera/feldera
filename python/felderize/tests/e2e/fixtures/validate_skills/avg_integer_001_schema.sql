-- rule: avg_integer
-- spark: AVG(int_col) — Spark returns DOUBLE (e.g. AVG(1,2) = 1.5); Feldera returns INT (= 1)
-- feldera: AVG(CAST(int_col AS DOUBLE)) — cast integer input to DOUBLE to match Spark's return type
CREATE TABLE sales_metrics (
  id INT,
  product_id INT,
  quantity INT,
  region STRING
);
