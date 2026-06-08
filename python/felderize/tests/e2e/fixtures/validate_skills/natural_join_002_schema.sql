-- rule: natural_join
-- spark: NATURAL JOIN — auto-join on all matching column names
-- feldera: NATURAL JOIN — same syntax, supported directly in Feldera
CREATE TABLE orders (order_id INT, customer_id INT, amount DECIMAL(10,2));
CREATE TABLE customers (customer_id INT, customer_name STRING, city STRING);
