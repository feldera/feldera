-- rule: arrays_overlap
-- spark: arrays_overlap(a, b) — true if arrays share any element
-- feldera: ARRAYS_OVERLAP(a, b)
CREATE TABLE product_tags (product_id INT, tag_array ARRAY<STRING>);
