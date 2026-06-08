CREATE OR REPLACE TEMP VIEW bm97_priority_sorting AS
SELECT order_id, priority, created_at FROM priority_orders
ORDER BY CASE priority WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END, created_at;
