CREATE TABLE creative_sales (
  region STRING,
  channel STRING,
  sku STRING,
  amount DECIMAL(12,2),
  units INT
) USING parquet;
