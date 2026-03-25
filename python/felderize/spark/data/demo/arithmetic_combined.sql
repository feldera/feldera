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
  value / NULLIF(divisor, 0)    AS safe_ratio,
  value - baseline              AS delta
FROM metrics;
