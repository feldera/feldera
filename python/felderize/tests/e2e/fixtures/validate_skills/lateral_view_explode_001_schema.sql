-- rule: lateral_view_explode
-- spark: LATERAL VIEW explode(arr) t AS item — unnest array column into rows
-- feldera: CROSS JOIN LATERAL UNNEST(arr) AS t(item)  or  FROM t, UNNEST(arr) AS u(item)
CREATE TABLE product_tags_001 (product_id INT, product_name STRING, tags ARRAY<STRING>);
