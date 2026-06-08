-- rule: bool_and_window
-- spark: every(bool_col) OVER (ORDER BY bool_col) / bool_and(bool_col) OVER (ORDER BY bool_col) — boolean aggregate used as window function with ORDER BY on a BOOLEAN column
-- feldera: UNSUPPORTED — Feldera compiler error: 'OVER currently cannot sort on columns with type BOOL'. Mark as unsupported when the window ORDER BY is on a BOOLEAN column.
CREATE OR REPLACE TEMP VIEW event_flags_v1 AS
SELECT
  event_id,
  is_active,
  every(is_active) OVER (ORDER BY is_active) AS all_active_so_far
FROM event_flags
ORDER BY event_id;
