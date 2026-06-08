-- rule: positive_negative
-- spark: positive(x) — unary no-op; negative(x) — unary negation
-- feldera: positive(x) → x; negative(x) → -x
CREATE TABLE financial_transactions (
  txn_id INT,
  amount BIGINT,
  category STRING
);
