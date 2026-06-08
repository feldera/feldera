-- rule: monotonically_increasing_id
-- spark: monotonically_increasing_id() — generates a unique, monotonically increasing 64-bit integer per row using partition metadata; nondeterministic across runs
-- feldera: UNSUPPORTED — Feldera has no equivalent; no stable row identity in streaming mode. Mark unsupported and suggest a surrogate key from existing columns or a sequence.
CREATE TABLE product_catalog (
  product_id INT,
  product_name STRING,
  category STRING,
  price DECIMAL(10, 2),
  stock_qty INT
);
