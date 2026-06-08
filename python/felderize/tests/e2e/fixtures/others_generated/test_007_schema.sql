CREATE TABLE shipments (
  shipment_id BIGINT,
  warehouse_id BIGINT,
  shipped_at TIMESTAMP,
  expected_at TIMESTAMP,
  delivered_at TIMESTAMP,
  shipping_mode STRING
) USING parquet;
