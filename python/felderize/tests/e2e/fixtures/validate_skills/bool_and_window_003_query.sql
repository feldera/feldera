-- rule: bool_and_window
-- spark: every(bool_col) OVER (ORDER BY bool_col) / bool_and(bool_col) OVER (ORDER BY bool_col) — boolean aggregate used as window function with ORDER BY on a BOOLEAN column
-- feldera: UNSUPPORTED — Feldera compiler error: 'OVER currently cannot sort on columns with type BOOL'. Mark as unsupported when the window ORDER BY is on a BOOLEAN column.
CREATE OR REPLACE TEMP VIEW feature_state_v3 AS
SELECT
  feature_id,
  enabled,
  version,
  user_id,
  every(enabled) OVER (ORDER BY enabled DESC) AS all_enabled_descending
FROM feature_state
ORDER BY feature_id;
