-- rule: positive_negative
-- spark: positive(x) — unary no-op; negative(x) — unary negation
-- feldera: positive(x) → x; negative(x) → -x
CREATE OR REPLACE TEMP VIEW price_view AS SELECT
  product_id,
  positive(base_price) AS original_price,
  negative(adjustment) AS discount_amount
FROM price_adjust;
