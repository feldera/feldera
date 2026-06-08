-- rule: full_outer_join
-- spark: FULL OUTER JOIN t ON cond — returns all rows from both tables, filling NULLs when there is no match
-- feldera: FULL OUTER JOIN t ON cond — fully supported in Feldera; pass through unchanged
CREATE OR REPLACE TEMP VIEW order_customer_v2 AS SELECT o.order_id, o.customer_id, o.order_amount, c.customer_name FROM orders_t2 o FULL OUTER JOIN customers_t2 c ON o.customer_id = c.customer_id;
