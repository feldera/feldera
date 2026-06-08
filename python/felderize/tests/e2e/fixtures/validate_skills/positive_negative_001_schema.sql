-- rule: positive_negative
-- spark: positive(x) — unary no-op; negative(x) — unary negation
-- feldera: positive(x) → x; negative(x) → -x
CREATE TABLE price_adjust (
  product_id INT,
  base_price DECIMAL(10,2),
  adjustment DECIMAL(10,2)
);
