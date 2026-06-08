CREATE OR REPLACE TEMP VIEW bm17_payment_reconciliation AS
SELECT
  COALESCE(a.payment_ref, b.payment_ref) AS payment_ref,
  COALESCE(a.order_id, b.order_id) AS order_id,
  a.amount AS amount_a,
  b.amount AS amount_b,
  CASE
    WHEN a.payment_ref IS NULL THEN 'MISSING_IN_A'
    WHEN b.payment_ref IS NULL THEN 'MISSING_IN_B'
    WHEN a.amount <> b.amount THEN 'AMOUNT_MISMATCH'
    ELSE 'MATCH'
  END AS reconciliation_status
FROM gateway_a_payments a
FULL OUTER JOIN gateway_b_payments b
  ON a.payment_ref = b.payment_ref;
