CREATE TABLE creative_baskets (
  basket_id BIGINT,
  skus ARRAY<STRING>,
  qtys ARRAY<INT>,
  nested ARRAY<ARRAY<INT>>
) USING parquet;
