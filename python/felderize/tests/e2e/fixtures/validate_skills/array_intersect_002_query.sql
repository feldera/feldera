-- rule: array_intersect
-- spark: array_intersect(a, b) — intersection of two arrays
-- feldera: ARRAY_INTERSECT(a, b)
CREATE OR REPLACE TEMP VIEW unchanged_products_v2 AS SELECT store_id, array_intersect(current_stock, previous_stock) AS common_product_ids FROM inventory_comparison_v2;
