-- rule: nvl
-- spark: NVL(a, b) — return b if a is NULL
-- feldera: COALESCE(a, b) — NVL not supported in Feldera
CREATE OR REPLACE TEMP VIEW order_details AS SELECT
  order_id,
  customer_name,
  NVL(discount_percent, CAST(0.00 AS DECIMAL(5,2))) AS applied_discount,
  NVL(notes, 'No special notes') AS order_notes
FROM order_transactions;
