-- rule: count_distinct_agg
-- spark: COUNT(DISTINCT col) — count distinct values in a group
-- feldera: COUNT(DISTINCT col) — works identically in Feldera, no translation needed
CREATE TABLE customer_orders (customer_id BIGINT, order_id BIGINT, category STRING);
