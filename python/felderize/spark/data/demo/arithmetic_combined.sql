-- Demo: pmod, try_divide, try_subtract
-- Covers: positive modulo, NULL-on-zero division, safe subtraction

CREATE TABLE metrics (
  metric_id  BIGINT,
  value      BIGINT,
  bucket     BIGINT,
  divisor    BIGINT,
  baseline   BIGINT
) USING parquet;

CREATE OR REPLACE TEMP VIEW metric_results AS
SELECT
  metric_id,
  pmod(value, bucket)           AS bucketed,
  try_divide(value, divisor)    AS safe_ratio,
  try_subtract(value, baseline) AS delta
FROM metrics;
