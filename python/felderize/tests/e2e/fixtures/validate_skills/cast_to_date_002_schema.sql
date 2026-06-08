-- rule: cast_to_date
-- spark: CAST(string AS DATE) — parse ISO date string
-- feldera: CAST(string AS DATE) — pass through unchanged
CREATE TABLE transaction_records (
  txn_id INT,
  txn_date_string STRING,
  amount DECIMAL(10, 2)
);
