CREATE OR REPLACE TEMP VIEW bm91_collect_order_ids AS
SELECT customer_id, collect_list(order_id) AS order_ids FROM customer_order_ids GROUP BY customer_id;
