CREATE OR REPLACE TEMP VIEW val188_json_parsed_and_exploded AS
SELECT row_id, item.sku AS sku, item.qty AS qty
FROM (SELECT row_id, from_json(raw_json, 'ARRAY<STRUCT<sku: STRING, qty: INT>>') AS items FROM collection_events) j
LATERAL VIEW explode(items) e AS item;
