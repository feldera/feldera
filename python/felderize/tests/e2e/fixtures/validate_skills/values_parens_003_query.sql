-- rule: values_parens
-- spark: SELECT ... FROM VALUES (1), (2) AS t(a) — VALUES without outer parentheses
-- feldera: SELECT ... FROM (VALUES (1), (2)) AS t(a) — wrap VALUES in parentheses
CREATE OR REPLACE TEMP VIEW order_status AS SELECT status_code, status_desc FROM VALUES (1, 'Pending'), (2, 'Shipped'), (3, 'Delivered'), (4, 'Cancelled'), (5, 'Returned') AS status_table(status_code, status_desc);
