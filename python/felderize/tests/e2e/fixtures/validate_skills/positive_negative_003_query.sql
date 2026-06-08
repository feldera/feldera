-- rule: positive_negative
-- spark: positive(x) — unary no-op; negative(x) — unary negation
-- feldera: positive(x) → x; negative(x) → -x
CREATE OR REPLACE TEMP VIEW txn_analysis AS SELECT
  txn_id,
  category,
  positive(amount) AS credited_amount,
  negative(amount) AS reversal_amount
FROM financial_transactions;
