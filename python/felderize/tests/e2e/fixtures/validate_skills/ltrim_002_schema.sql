-- rule: ltrim
-- spark: LTRIM(s) — removes leading whitespace
-- feldera: TRIM(LEADING FROM s)
CREATE TABLE user_bios (user_id INT, bio STRING);
