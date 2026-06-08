CREATE OR REPLACE TEMP VIEW bm41_repeat_buyers AS
WITH monthly_orders AS (
  SELECT customer_id, date_trunc('MONTH', CAST(order_date AS TIMESTAMP)) AS month_start, COUNT(*) AS order_count
  FROM customer_orders GROUP BY customer_id, date_trunc('MONTH', CAST(order_date AS TIMESTAMP))
), repeat_months AS (
  SELECT customer_id, month_start FROM monthly_orders WHERE order_count >= 2
)
SELECT customer_id, COUNT(*) AS repeat_month_count FROM repeat_months GROUP BY customer_id;
