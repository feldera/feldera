-- rule: join_using
-- spark: JOIN ... USING (col) — join on same-named column
-- feldera: JOIN ... USING (col) — same syntax, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW order_customer_view_v2 AS SELECT o.order_id, c.customer_name, o.amount, c.city FROM orders_t2 o JOIN customers_t2 c USING (customer_id);
