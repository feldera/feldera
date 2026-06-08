CREATE OR REPLACE TEMP VIEW daily_sales_v3 AS SELECT
  transaction_id,
  product_name,
  sale_amount,
  transaction_date,
  DATE '2026-03-30' AS processed_date,
  CASE WHEN transaction_date = DATE '2026-03-30' THEN 'today' ELSE 'past' END AS sale_type
FROM sales_transactions
ORDER BY sale_amount DESC;
