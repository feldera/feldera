-- rule: lateral_view_explode
-- spark: LATERAL VIEW explode(arr) t AS item — unnest array column into rows
-- feldera: CROSS JOIN LATERAL UNNEST(arr) AS t(item)  or  FROM t, UNNEST(arr) AS u(item)
CREATE OR REPLACE TEMP VIEW exploded_tags_v1 AS SELECT product_id, product_name, tag FROM product_tags_001 LATERAL VIEW explode(tags) t AS tag;
