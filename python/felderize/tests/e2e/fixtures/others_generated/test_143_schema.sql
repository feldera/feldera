CREATE TABLE quarterly_sales (
  product_id BIGINT,
  product_name STRING,
  q1_revenue DOUBLE,
  q2_revenue DOUBLE,
  q3_revenue DOUBLE,
  q4_revenue DOUBLE
) USING parquet;
