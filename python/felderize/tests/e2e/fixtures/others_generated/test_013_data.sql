INSERT INTO customer_master (customer_id, customer_name, signup_date, country) VALUES (1, 'Alice Johnson', CAST('2023-01-15' AS DATE), 'USA');
INSERT INTO customer_master (customer_id, customer_name, signup_date, country) VALUES (2, 'Bob Smith', CAST('2023-02-20' AS DATE), 'Canada');
INSERT INTO customer_master (customer_id, customer_name, signup_date, country) VALUES (3, 'Charlie Brown', CAST('2023-03-10' AS DATE), 'UK');
INSERT INTO customer_master (customer_id, customer_name, signup_date, country) VALUES (4, 'Diana Prince', CAST('2023-04-05' AS DATE), 'USA');
INSERT INTO customer_master (customer_id, customer_name, signup_date, country) VALUES (5, 'Eve Wilson', CAST('2023-05-12' AS DATE), 'Australia');
INSERT INTO recent_orders (order_id, customer_id, order_date, amount) VALUES (101, 2, CAST('2025-12-15' AS DATE), 150.50);
INSERT INTO recent_orders (order_id, customer_id, order_date, amount) VALUES (102, 3, CAST('2025-11-20' AS DATE), 200.00);
INSERT INTO recent_orders (order_id, customer_id, order_date, amount) VALUES (103, 5, CAST('2025-10-30' AS DATE), 75.25);
