-- rule: zeroifnull
-- spark: ZEROIFNULL(x) — return 0 if x is NULL
-- feldera: COALESCE(x, 0)
CREATE OR REPLACE TEMP VIEW sales_summary AS SELECT
  transaction_id,
  ZEROIFNULL(amount) AS amount_zero_safe,
  ZEROIFNULL(discount) AS discount_zero_safe
FROM sales_metrics;
