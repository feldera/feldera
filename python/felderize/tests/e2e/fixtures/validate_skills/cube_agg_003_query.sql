-- rule: cube_agg
-- spark: CUBE(a, b) — all combinations of grouping
-- feldera: CUBE(a, b) — same
CREATE OR REPLACE TEMP VIEW cube_agg_results_003 AS
SELECT
  customer_type,
  payment_method,
  COUNT(*) as transaction_count,
  SUM(transaction_amount) as total_amount,
  MAX(transaction_amount) as max_amount
FROM transaction_log
GROUP BY CUBE(customer_type, payment_method)
ORDER BY customer_type, payment_method;
