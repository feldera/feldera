CREATE OR REPLACE TEMP VIEW bm10_paid_user_activity_sets AS
WITH active_last_7_days AS (
  SELECT user_id
  FROM daily_active_users
  WHERE activity_date >= DATE '2026-03-01'
),
pro_users AS (
  SELECT user_id
  FROM paid_users
  WHERE plan_name = 'PRO'
),
active_pro_users AS (
  SELECT user_id FROM active_last_7_days
  INTERSECT
  SELECT user_id FROM pro_users
),
inactive_pro_users AS (
  SELECT user_id FROM pro_users
  EXCEPT
  SELECT user_id FROM active_last_7_days
)
SELECT 'ACTIVE_PRO' AS segment, user_id FROM active_pro_users
UNION ALL
SELECT 'INACTIVE_PRO' AS segment, user_id FROM inactive_pro_users;
