-- rule: bool_or_and_agg
-- spark: bool_or(col) — true if any value is true; bool_and(col) — true if all values are true
-- feldera: bool_or(col) / bool_and(col) — both work identically in Feldera as aggregates (not window); no translation needed
CREATE OR REPLACE TEMP VIEW device_health_v2 AS SELECT department, bool_or(is_online) AS has_online, bool_and(is_online) AS all_online FROM device_status GROUP BY department;
