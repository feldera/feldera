CREATE OR REPLACE TEMP VIEW bm545_creative_bucket_greatest AS
SELECT
  row_id,
  width_bucket(x, 0.0, 100.0, 10) AS decile_bin,
  least(x, 50.0, 75.0) AS clamp_low,
  greatest(x, 1.0, 2.0) AS clamp_high
FROM creative_nums;
