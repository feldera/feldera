-- rule: every_some_agg
-- spark: SELECT every(is_active) AS all_active, some(is_active) AS any_active FROM t — boolean aggregate functions
-- feldera: every(col) → same (alias for bool_and, supported); some(col) → same (supported); any(col) → bool_or(col) (any is reserved in Feldera)
CREATE OR REPLACE TEMP VIEW feature_status_check AS
SELECT
  every(enabled) AS all_features_enabled,
  some(enabled) AS any_feature_enabled
FROM feature_flags;
