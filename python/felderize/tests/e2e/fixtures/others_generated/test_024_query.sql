CREATE OR REPLACE TEMP VIEW bm34_running_refunds AS
SELECT account_id, entry_time,
  SUM(CASE WHEN entry_type = 'REFUND' THEN amount ELSE 0 END) OVER (PARTITION BY account_id ORDER BY entry_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_refund_amount
FROM ledger_entries;
