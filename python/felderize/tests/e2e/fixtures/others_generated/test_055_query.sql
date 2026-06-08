CREATE OR REPLACE TEMP VIEW bm82_combined_alerts AS
SELECT alert_id, severity, created_at FROM (
  SELECT alert_id, severity, created_at FROM system_alerts
  UNION ALL
  SELECT alert_id, severity, created_at FROM security_alerts
) a ORDER BY created_at DESC;
