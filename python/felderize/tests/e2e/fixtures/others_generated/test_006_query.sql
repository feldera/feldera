CREATE OR REPLACE TEMP VIEW bm06_customer_payment_windows AS
SELECT
  customer_id,
  payment_time,
  amount,
  ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY payment_time
  ) AS payment_number,
  LAG(amount) OVER (
    PARTITION BY customer_id
    ORDER BY payment_time
  ) AS previous_amount,
  SUM(amount) OVER (
    PARTITION BY customer_id
    ORDER BY payment_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_amount
FROM payments;
