CREATE OR REPLACE TEMP VIEW transaction_analytics AS
SELECT
  txn_id,
  account_id,
  amount,
  LAG(amount) OVER (PARTITION BY account_id ORDER BY txn_time) AS prev_amount,
  SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM transactions;
