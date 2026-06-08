-- rule: any_agg
-- spark: any(col) — true if any value in group is true (Spark aggregate)
-- feldera: bool_or(col) — 'any' is a reserved keyword in Feldera
CREATE TABLE data_quality_checks (check_id INT, dataset_name STRING, passed BOOLEAN, check_timestamp TIMESTAMP);
