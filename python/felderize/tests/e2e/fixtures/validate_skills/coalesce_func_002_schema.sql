-- rule: coalesce_func
-- spark: COALESCE(expr1, expr2, ...) — return first non-NULL value
-- feldera: COALESCE(expr1, expr2, ...) — works identically in Feldera, no translation needed
CREATE TABLE price_data (
  product_id INT,
  list_price DOUBLE,
  discount_price DOUBLE,
  sale_price DOUBLE
);
