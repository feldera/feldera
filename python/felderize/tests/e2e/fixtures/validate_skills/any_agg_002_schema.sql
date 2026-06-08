-- rule: any_agg
-- spark: any(col) — true if any value in group is true (Spark aggregate)
-- feldera: bool_or(col) — 'any' is a reserved keyword in Feldera
CREATE TABLE feature_flags (feature_name STRING, environment STRING, is_enabled BOOLEAN); CREATE TABLE teams (team_id INT, team_name STRING);
