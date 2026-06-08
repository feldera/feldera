-- rule: rtrim
-- spark: RTRIM(s) — removes trailing whitespace
-- feldera: TRIM(TRAILING FROM s)
CREATE TABLE descriptions (item_id INT, desc STRING);
