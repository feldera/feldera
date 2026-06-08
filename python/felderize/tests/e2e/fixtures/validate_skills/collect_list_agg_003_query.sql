-- rule: collect_list_agg
-- spark: collect_list(col) — aggregate all column values per group into an array (preserves duplicates)
-- feldera: ARRAY_AGG(col) — order may differ when source collection is unordered
CREATE OR REPLACE TEMP VIEW order_item_codes_v3 AS SELECT order_id, collect_list(item_code) as items_ordered FROM order_items GROUP BY order_id;
