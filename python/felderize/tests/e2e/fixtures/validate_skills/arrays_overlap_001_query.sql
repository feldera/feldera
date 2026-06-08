-- rule: arrays_overlap
-- spark: arrays_overlap(a, b) — true if arrays share any element
-- feldera: ARRAYS_OVERLAP(a, b)
CREATE OR REPLACE TEMP VIEW overlap_check_v1 AS SELECT product_id, tag_array, arrays_overlap(tag_array, array('popular', 'featured')) AS is_featured FROM product_tags;
