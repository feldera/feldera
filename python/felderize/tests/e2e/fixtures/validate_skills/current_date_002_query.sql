CREATE OR REPLACE TEMP VIEW registration_status_v2 AS SELECT
  user_id,
  username,
  registration_date,
  DATE '2026-03-30' AS check_date,
  DATEDIFF(DATE '2026-03-30', registration_date) AS days_active
FROM user_registrations
WHERE status = 'active';
