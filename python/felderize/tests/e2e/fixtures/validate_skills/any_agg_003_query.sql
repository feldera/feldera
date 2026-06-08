-- rule: any_agg
-- spark: any(col) — true if any value in group is true (Spark aggregate)
-- feldera: bool_or(col) — 'any' is a reserved keyword in Feldera
CREATE OR REPLACE TEMP VIEW any_agg_v3 AS SELECT dataset_name, any(passed) AS any_passed FROM data_quality_checks GROUP BY dataset_name ORDER BY dataset_name;
