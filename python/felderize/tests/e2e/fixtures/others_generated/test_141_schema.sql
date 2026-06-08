CREATE TABLE audio_levels (
  sample_id BIGINT,
  channel STRING,
  amplitude DOUBLE
) USING parquet;
