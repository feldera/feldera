-- rule: values_parens
-- spark: SELECT ... FROM VALUES (1), (2) AS t(a) — VALUES without outer parentheses
-- feldera: SELECT ... FROM (VALUES (1), (2)) AS t(a) — wrap VALUES in parentheses
CREATE OR REPLACE TEMP VIEW categories AS SELECT cat_id, cat_name FROM VALUES (1, 'Electronics'), (2, 'Clothing'), (3, 'Food'), (4, 'Books') AS cat_table(cat_id, cat_name);
