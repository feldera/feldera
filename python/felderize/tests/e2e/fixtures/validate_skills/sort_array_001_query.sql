-- rule: sort_array
-- spark: sort_array(arr) / sort_array(arr, false) — sort array ascending or descending
-- feldera: SORT_ARRAY(arr) / SORT_ARRAY(arr, false)
CREATE OR REPLACE TEMP VIEW sorted_quantities_v1 AS SELECT id, product_name, sort_array(quantities) AS sorted_asc, sort_array(quantities, false) AS sorted_desc FROM numbers_inventory;
