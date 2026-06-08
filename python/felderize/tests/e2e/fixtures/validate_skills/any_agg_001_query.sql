-- rule: any_agg
-- spark: any(col) — true if any value in group is true (Spark aggregate)
-- feldera: bool_or(col) — 'any' is a reserved keyword in Feldera
CREATE OR REPLACE TEMP VIEW any_agg_v1 AS SELECT u.user_id, u.username, any(la.is_successful) AS had_successful_login FROM login_attempts la JOIN users u ON la.user_id = u.user_id GROUP BY u.user_id, u.username;
