-- rule: any_agg
-- spark: any(col) — true if any value in group is true (Spark aggregate)
-- feldera: bool_or(col) — 'any' is a reserved keyword in Feldera
CREATE OR REPLACE TEMP VIEW any_agg_v2 AS SELECT feature_name, any(is_enabled) AS enabled_anywhere FROM feature_flags GROUP BY feature_name;
