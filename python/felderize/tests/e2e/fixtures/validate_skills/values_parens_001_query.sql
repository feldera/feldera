-- rule: values_parens
-- spark: SELECT ... FROM VALUES (1), (2) AS t(a) — VALUES without outer parentheses
-- feldera: SELECT ... FROM (VALUES (1), (2)) AS t(a) — wrap VALUES in parentheses
CREATE OR REPLACE TEMP VIEW salary_grades AS SELECT grade, count_val FROM VALUES ('A', 5), ('B', 3), ('C', 2) AS grades(grade, count_val);
