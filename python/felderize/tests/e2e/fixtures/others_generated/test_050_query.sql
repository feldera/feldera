CREATE OR REPLACE TEMP VIEW bm72_parsed_order_payload AS
SELECT order_id, from_json(payload, 'customer STRUCT<id: BIGINT, tier: STRING>, amount DECIMAL(12,2)') AS parsed_payload FROM json_orders;
