CREATE OR REPLACE TEMP VIEW bm21_checkout_json_projection AS
SELECT
  CAST(get_json_object(payload, '$.user.id') AS BIGINT) AS user_id,
  get_json_object(payload, '$.event.type') AS event_type,
  CAST(get_json_object(payload, '$.order.amount') AS DECIMAL(12,2)) AS order_amount
FROM raw_api_events
WHERE get_json_object(payload, '$.event.type') = 'checkout';
