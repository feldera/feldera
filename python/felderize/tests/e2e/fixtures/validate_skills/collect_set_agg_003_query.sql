-- rule: collect_set_agg
-- spark: collect_set(col) — aggregate distinct column values per group into an array
-- feldera: ARRAY_AGG(DISTINCT col)
CREATE OR REPLACE TEMP VIEW order_composition_v3 AS SELECT order_id, collect_set(item_code) AS item_codes FROM order_items GROUP BY order_id ORDER BY order_id;
