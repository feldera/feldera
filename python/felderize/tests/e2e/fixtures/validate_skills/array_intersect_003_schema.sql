-- rule: array_intersect
-- spark: array_intersect(a, b) — intersection of two arrays
-- feldera: ARRAY_INTERSECT(a, b)
CREATE TABLE customer_purchases_v3 (customer_id INT, jan_items ARRAY<STRING>, feb_items ARRAY<STRING>);
