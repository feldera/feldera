CREATE OR REPLACE TEMP VIEW val190_map_concat_attrs AS
SELECT row_id, map_concat(attrs, attrs2) AS merged_attrs
FROM collection_events
WHERE map_contains_key(attrs, 'campaign');
