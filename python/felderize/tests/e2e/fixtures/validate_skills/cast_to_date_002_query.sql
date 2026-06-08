-- rule: cast_to_date
-- spark: CAST(string AS DATE) — parse ISO date string
-- feldera: CAST(string AS DATE) — pass through unchanged
CREATE OR REPLACE TEMP VIEW transaction_dates AS SELECT
  txn_id,
  CAST(txn_date_string AS DATE) AS transaction_date,
  amount
FROM transaction_records;
