CREATE TABLE feldera_edge_wide_metrics (
  entity_id BIGINT,
  q1_amt DECIMAL(10,2),
  q2_amt DECIMAL(10,2),
  q3_amt DECIMAL(10,2)
) USING parquet;
