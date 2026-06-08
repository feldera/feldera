-- rule: array_literal
-- spark: array(v1, v2, ...) — construct an array literal from values
-- feldera: ARRAY(v1, v2, ...) — same syntax in Feldera; also ARRAY[v1, v2, ...] works
CREATE OR REPLACE TEMP VIEW tags_with_array AS SELECT product_id, array(tag_string, 'general', 'sale') AS tag_list FROM product_tags;
