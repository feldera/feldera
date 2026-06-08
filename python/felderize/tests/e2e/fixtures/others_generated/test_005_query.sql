CREATE OR REPLACE TEMP VIEW bm05_high_risk_accounts AS
SELECT
  a.account_id,
  a.customer_id
FROM accounts a
WHERE a.account_status = 'ACTIVE'
  AND EXISTS (
    SELECT 1
    FROM transactions t
    WHERE t.account_id = a.account_id
      AND t.amount > 1000
  )
  AND a.account_id IN (
    SELECT d.account_id
    FROM disputes d
    WHERE d.status = 'OPEN'
  );
