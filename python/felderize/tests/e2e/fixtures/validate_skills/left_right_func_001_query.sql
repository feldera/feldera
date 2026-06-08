-- rule: left_right_func
-- spark: LEFT(s, n) — first n characters; RIGHT(s, n) — last n characters
-- feldera: LEFT(s, n) / RIGHT(s, n) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW product_view AS SELECT id, LEFT(code, 3) AS prefix, RIGHT(code, 2) AS suffix, description FROM product_codes;
