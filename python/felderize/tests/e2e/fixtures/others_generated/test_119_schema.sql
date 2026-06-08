CREATE TABLE collection_events (
  row_id BIGINT,
  grp STRING,
  nums ARRAY<INT>,
  nums2 ARRAY<INT>,
  prices ARRAY<DECIMAL(10,2)>,
  taxes ARRAY<DECIMAL(10,2)>,
  tags ARRAY<STRING>,
  tags2 ARRAY<STRING>,
  attrs MAP<STRING, STRING>,
  attrs2 MAP<STRING, STRING>,
  raw_json STRING,
  start_date DATE,
  end_date DATE
) USING parquet;
