CREATE OR REPLACE TEMP VIEW gpt140_sign AS
SELECT
  txn_id,
  description,
  amount,
  SIGN(amount) AS amount_sign
FROM transactions;
