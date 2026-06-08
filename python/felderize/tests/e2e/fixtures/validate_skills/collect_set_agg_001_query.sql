-- rule: collect_set_agg
-- spark: collect_set(col) — aggregate distinct column values per group into an array
-- feldera: ARRAY_AGG(DISTINCT col)
CREATE OR REPLACE TEMP VIEW skill_collection_v1 AS SELECT dept_id, collect_set(skill_name) AS skills FROM dept_skills GROUP BY dept_id;
