-- rule: array_intersect
-- spark: array_intersect(a, b) — intersection of two arrays
-- feldera: ARRAY_INTERSECT(a, b)
CREATE TABLE inventory_comparison_v2 (store_id INT, current_stock ARRAY<INT>, previous_stock ARRAY<INT>);
