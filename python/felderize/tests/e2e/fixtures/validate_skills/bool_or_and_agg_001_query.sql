-- rule: bool_or_and_agg
-- spark: bool_or(col) — true if any value is true; bool_and(col) — true if all values are true
-- feldera: bool_or(col) / bool_and(col) — both work identically in Feldera as aggregates (not window); no translation needed
CREATE OR REPLACE TEMP VIEW flag_summary_v1 AS SELECT region, bool_or(is_active) AS any_active, bool_and(is_active) AS all_active FROM flag_events GROUP BY region;
