CREATE OR REPLACE TEMP VIEW val21_item_count AS
SELECT row_id, json_array_length(raw_json) AS item_count
FROM collection_events;
