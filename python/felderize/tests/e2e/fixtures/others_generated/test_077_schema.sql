CREATE TABLE inventory_snapshot (
  warehouse_id BIGINT,
  sku STRING,
  on_hand INT,
  flagged BOOLEAN
) USING parquet;

CREATE TABLE inventory_flags (
  warehouse_id BIGINT,
  sku STRING,
  flagged BOOLEAN,
  review_status STRING
) USING parquet;
