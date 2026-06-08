-- rule: lateral_view_explode_map
-- spark: LATERAL VIEW explode(map_col) t AS key, val — unnest map into key/value rows
-- feldera: CROSS JOIN UNNEST(map_col) AS t(key, val)
CREATE OR REPLACE TEMP VIEW inventory_properties_v3 AS
SELECT im.item_id, im.item_name, im.quantity, t.key AS prop_key, t.val AS prop_val
FROM inventory_metadata im
LATERAL VIEW explode(im.properties) t AS key, val;
