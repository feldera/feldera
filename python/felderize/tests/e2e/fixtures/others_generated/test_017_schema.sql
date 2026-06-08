CREATE TABLE store_product_sales (
  store_id BIGINT,
  product_id BIGINT,
  sale_date DATE,
  revenue DECIMAL(12,2)
) USING parquet;
