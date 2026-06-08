CREATE OR REPLACE TEMP VIEW bm46_order_size_buckets AS
SELECT order_id,
  CASE WHEN amount < 50 THEN 'SMALL' WHEN amount < 250 THEN 'MEDIUM' WHEN amount < 1000 THEN 'LARGE' ELSE 'ENTERPRISE' END AS amount_bucket
FROM sized_orders;
