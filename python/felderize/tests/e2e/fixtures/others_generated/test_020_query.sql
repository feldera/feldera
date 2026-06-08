CREATE OR REPLACE TEMP VIEW bm27_balance_snapshots AS
SELECT account_id, snapshot_time, balance,
  FIRST_VALUE(balance) OVER (PARTITION BY account_id ORDER BY snapshot_time) AS opening_balance,
  LAST_VALUE(balance) OVER (PARTITION BY account_id ORDER BY snapshot_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS closing_balance
FROM balance_history;
