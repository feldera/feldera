CREATE TABLE window_scores (
  grp STRING,
  item_id BIGINT,
  score DECIMAL(12,2),
  created_at TIMESTAMP
) USING parquet;
