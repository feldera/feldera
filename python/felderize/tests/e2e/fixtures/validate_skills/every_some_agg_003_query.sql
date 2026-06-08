-- rule: every_some_agg
-- spark: SELECT every(is_active) AS all_active, some(is_active) AS any_active FROM t — boolean aggregate functions
-- feldera: every(col) → same (alias for bool_and, supported); some(col) → same (supported); any(col) → bool_or(col) (any is reserved in Feldera)
CREATE OR REPLACE TEMP VIEW system_health_report AS
SELECT
  every(is_healthy) AS all_devices_healthy,
  some(is_healthy) AS any_device_healthy
FROM device_health;
