CREATE OR REPLACE TEMP VIEW bm100_mixed_feature_summary AS
SELECT
  REGEXP_EXTRACT(event_name, '([A-Za-z_]+)', 1) AS event_prefix,
  CASE WHEN amount >= 100 THEN 'LARGE' ELSE 'SMALL' END AS amount_bucket,
  COUNT(*) AS event_count,
  SUM(amount) AS total_amount
FROM mixed_events
GROUP BY REGEXP_EXTRACT(event_name, '([A-Za-z_]+)', 1), CASE WHEN amount >= 100 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) > 0;
