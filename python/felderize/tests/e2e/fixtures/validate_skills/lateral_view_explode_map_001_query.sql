-- rule: lateral_view_explode_map
-- spark: LATERAL VIEW explode(map_col) t AS key, val — unnest map into key/value rows
-- feldera: CROSS JOIN UNNEST(map_col) AS t(key, val)
CREATE OR REPLACE TEMP VIEW product_exploded_v1 AS
SELECT pa.product_id, pa.product_name, t.key AS attr_key, t.val AS attr_value
FROM product_attrs pa
LATERAL VIEW explode(pa.attributes) t AS key, val;
