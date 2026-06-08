-- rule: collect_set_agg
-- spark: collect_set(col) — aggregate distinct column values per group into an array
-- feldera: ARRAY_AGG(DISTINCT col)
CREATE TABLE order_items (order_id INT, item_code STRING);
CREATE TABLE order_details (order_id INT, item_codes ARRAY<STRING>);
