CREATE OR REPLACE TEMP VIEW bm53_casted_metrics AS
SELECT metric_id, CAST(metric_value AS DOUBLE) AS metric_value_double, CAST(CAST(metric_value AS DOUBLE) AS BIGINT) AS metric_value_bigint FROM string_metrics;
