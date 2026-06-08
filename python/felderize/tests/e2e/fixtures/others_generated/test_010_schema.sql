CREATE TABLE session_events (
  session_id BIGINT,
  user_id BIGINT,
  event_time TIMESTAMP,
  items ARRAY<STRUCT<sku: STRING, qty: INT, price: DECIMAL(12,2)>>,
  tags ARRAY<STRING>,
  attributes MAP<STRING, STRING>
) USING parquet;
