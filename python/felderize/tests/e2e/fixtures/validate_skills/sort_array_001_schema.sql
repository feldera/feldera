-- rule: sort_array
-- spark: sort_array(arr) / sort_array(arr, false) — sort array ascending or descending
-- feldera: SORT_ARRAY(arr) / SORT_ARRAY(arr, false)
CREATE TABLE numbers_inventory (id INT, product_name STRING, quantities ARRAY<INT>);
