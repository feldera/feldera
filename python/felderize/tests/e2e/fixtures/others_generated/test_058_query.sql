CREATE OR REPLACE TEMP VIEW bm93_metric_payloads AS
SELECT metric_name, named_struct('min_value', MIN(metric_value), 'max_value', MAX(metric_value)) AS bounds FROM metric_points GROUP BY metric_name;
