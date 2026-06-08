-- rule: array_remove
-- spark: array_remove(arr, val) — remove all occurrences of val from array
-- feldera: ARRAY_REMOVE(arr, val)
CREATE OR REPLACE TEMP VIEW cleaned_tags_v1 AS SELECT item_id, array_remove(tags, 'deprecated') AS filtered_tags FROM inventory_items;
