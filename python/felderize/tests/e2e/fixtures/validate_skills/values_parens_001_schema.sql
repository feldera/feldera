-- rule: values_parens
-- spark: SELECT ... FROM VALUES (1), (2) AS t(a) — VALUES without outer parentheses
-- feldera: SELECT ... FROM (VALUES (1), (2)) AS t(a) — wrap VALUES in parentheses
CREATE TABLE employee (id INT, name STRING, salary DECIMAL(10,2));
