-- rule: collect_set_agg
-- spark: collect_set(col) — aggregate distinct column values per group into an array
-- feldera: ARRAY_AGG(DISTINCT col)
CREATE TABLE dept_skills (dept_id INT, skill_name STRING);
CREATE TABLE dept_summary (dept_id INT, skills ARRAY<STRING>);
