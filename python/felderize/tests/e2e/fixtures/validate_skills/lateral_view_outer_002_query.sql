-- rule: lateral_view_outer
-- spark: LATERAL VIEW OUTER explode(arr) t AS item — outer explode: preserves rows where array is NULL or empty (returns NULL for item column)
-- feldera: UNSUPPORTED — Feldera has no OUTER equivalent. UNNEST drops rows where array is NULL or empty. Mark as unsupported and add a warning comment.
CREATE OR REPLACE TEMP VIEW event_parameters_flattened AS
SELECT
  event_id,
  user_id,
  event_name,
  param
FROM event_log
LATERAL VIEW OUTER explode(parameters) params AS param;
