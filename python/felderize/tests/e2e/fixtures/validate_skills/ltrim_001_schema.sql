-- rule: ltrim
-- spark: LTRIM(s) — removes leading whitespace
-- feldera: TRIM(LEADING FROM s)
CREATE TABLE product_names (id INT, name STRING);
