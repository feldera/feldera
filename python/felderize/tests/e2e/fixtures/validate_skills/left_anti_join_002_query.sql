-- rule: left_anti_join
-- spark: LEFT ANTI JOIN t2 ON cond — rows in t1 with no match in t2
-- feldera: WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE cond)
CREATE OR REPLACE TEMP VIEW active_orders_v2 AS
SELECT o.order_id, o.customer_id, o.amount
FROM orders_main o
LEFT ANTI JOIN cancelled_orders c ON o.order_id = c.order_id;
