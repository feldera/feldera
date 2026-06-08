CREATE OR REPLACE TEMP VIEW bm532_creative_try_math AS
SELECT
  row_id,
  try_add(a, b) AS safe_sum,
  try_divide(CAST(a AS DOUBLE), CAST(b AS DOUBLE)) AS safe_ratio,
  coalesce(try_multiply(a, 0), CAST(0 AS BIGINT)) AS safe_mul
FROM creative_bits;
