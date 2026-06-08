-- rule: float_type
-- spark: FLOAT type in CREATE TABLE DDL
-- feldera: REAL — Feldera uses REAL instead of FLOAT
CREATE OR REPLACE TEMP VIEW efficiency_report_v3 AS SELECT location, AVG(watts_used) as avg_watts, MIN(efficiency_ratio) as min_efficiency, MAX(efficiency_ratio) as max_efficiency FROM energy_consumption GROUP BY location HAVING AVG(watts_used) > 500.0;
