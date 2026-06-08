CREATE TABLE creative_line_items (
  order_id BIGINT,
  lines ARRAY<STRUCT<sku: STRING, qty: INT, price: DECIMAL(10,2)>>
) USING parquet;
