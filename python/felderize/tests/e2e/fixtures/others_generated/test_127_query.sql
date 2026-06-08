CREATE OR REPLACE TEMP VIEW bm534_creative_make_extract AS
SELECT
  row_id,
  make_timestamp(2024, 6, 1, 12, 30, CAST(0.0 AS DOUBLE)) AS noon_june,
  extract(YEAR FROM ts) AS y,
  dayofweek(d) AS dow_spark
FROM creative_ts;
