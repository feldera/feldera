CREATE OR REPLACE TEMP VIEW val169_order_status_union AS
SELECT order_id, customer_id, amount, 'OPEN' AS status_group FROM order_fact WHERE status = 'OPEN'
UNION ALL
SELECT order_id, customer_id, amount, 'CLOSED' AS status_group FROM order_fact WHERE status = 'CLOSED';
