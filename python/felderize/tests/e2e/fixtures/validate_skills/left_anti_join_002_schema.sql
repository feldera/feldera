-- rule: left_anti_join
-- spark: LEFT ANTI JOIN t2 ON cond — rows in t1 with no match in t2
-- feldera: WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE cond)
CREATE TABLE orders_main (order_id INT, customer_id INT, amount DECIMAL(10,2));
CREATE TABLE cancelled_orders (order_id INT);
