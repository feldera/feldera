-- rule: left_right_func
-- spark: LEFT(s, n) — first n characters; RIGHT(s, n) — last n characters
-- feldera: LEFT(s, n) / RIGHT(s, n) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW username_extract AS SELECT user_id, LEFT(full_name, 1) AS first_initial, RIGHT(full_name, 4) AS last_four_chars, email FROM user_names WHERE LENGTH(full_name) >= 4;
