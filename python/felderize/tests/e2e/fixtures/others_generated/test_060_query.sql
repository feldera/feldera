CREATE OR REPLACE TEMP VIEW bm95_json_item_count AS
SELECT event_id, size(from_json(payload, 'ARRAY<STRING>')) AS item_count FROM json_items;
