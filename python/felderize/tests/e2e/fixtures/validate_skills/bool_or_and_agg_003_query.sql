-- rule: bool_or_and_agg
-- spark: bool_or(col) — true if any value is true; bool_and(col) — true if all values are true
-- feldera: bool_or(col) / bool_and(col) — both work identically in Feldera as aggregates (not window); no translation needed
CREATE OR REPLACE TEMP VIEW permission_check_v3 AS SELECT team, bool_or(has_permission) AS any_permitted, bool_and(has_permission) AS all_permitted FROM permission_audit GROUP BY team;
