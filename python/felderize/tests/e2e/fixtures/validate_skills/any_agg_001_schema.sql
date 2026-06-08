-- rule: any_agg
-- spark: any(col) — true if any value in group is true (Spark aggregate)
-- feldera: bool_or(col) — 'any' is a reserved keyword in Feldera
CREATE TABLE login_attempts (user_id INT, session_id STRING, is_successful BOOLEAN); CREATE TABLE users (user_id INT, username STRING);
