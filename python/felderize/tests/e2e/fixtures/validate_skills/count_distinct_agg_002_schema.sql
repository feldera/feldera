-- rule: count_distinct_agg
-- spark: COUNT(DISTINCT col) — count distinct values in a group
-- feldera: COUNT(DISTINCT col) — works identically in Feldera, no translation needed
CREATE TABLE sales_data (region STRING, product_id INT, amount DOUBLE);
