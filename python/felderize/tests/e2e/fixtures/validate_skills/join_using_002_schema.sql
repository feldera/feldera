-- rule: join_using
-- spark: JOIN ... USING (col) — join on same-named column
-- feldera: JOIN ... USING (col) — same syntax, supported directly in Feldera
CREATE TABLE orders_t2 (order_id INT, customer_id INT, amount DECIMAL(10,2));
CREATE TABLE customers_t2 (customer_id INT, customer_name STRING, city STRING);
