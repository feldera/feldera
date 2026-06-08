-- rule: left_right_func
-- spark: LEFT(s, n) — first n characters; RIGHT(s, n) — last n characters
-- feldera: LEFT(s, n) / RIGHT(s, n) — both work identically in Feldera, no translation needed
CREATE TABLE user_names (user_id INT, full_name STRING, email STRING);
