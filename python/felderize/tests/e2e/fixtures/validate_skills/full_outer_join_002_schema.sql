-- rule: full_outer_join
-- spark: FULL OUTER JOIN t ON cond — returns all rows from both tables, filling NULLs when there is no match
-- feldera: FULL OUTER JOIN t ON cond — fully supported in Feldera; pass through unchanged
CREATE TABLE orders_t2 (order_id INT, customer_id INT, order_amount DOUBLE);
CREATE TABLE customers_t2 (customer_id INT, customer_name STRING);
