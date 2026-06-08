-- rule: array_intersect
-- spark: array_intersect(a, b) — intersection of two arrays
-- feldera: ARRAY_INTERSECT(a, b)
CREATE OR REPLACE TEMP VIEW repeat_purchases_v3 AS SELECT customer_id, array_intersect(jan_items, feb_items) AS purchased_both_months FROM customer_purchases_v3;
