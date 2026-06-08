CREATE TABLE product_search (
  product_id BIGINT,
  query_term STRING,
  catalog_name STRING
) USING parquet;
