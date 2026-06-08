-- rule: values_parens
-- spark: SELECT ... FROM VALUES (1), (2) AS t(a) — VALUES without outer parentheses
-- feldera: SELECT ... FROM (VALUES (1), (2)) AS t(a) — wrap VALUES in parentheses
CREATE TABLE product (product_id INT, category STRING);
