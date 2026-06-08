-- rule: nvl
-- spark: NVL(a, b) — return b if a is NULL
-- feldera: COALESCE(a, b) — NVL not supported in Feldera
CREATE TABLE product_catalog (
  product_id INT,
  product_name STRING,
  category STRING,
  stock_qty INT
);
