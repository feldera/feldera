-- rule: collect_set_agg
-- spark: collect_set(col) — aggregate distinct column values per group into an array
-- feldera: ARRAY_AGG(DISTINCT col)
CREATE OR REPLACE TEMP VIEW user_interests_v2 AS SELECT user_id, collect_set(tag) AS tag_list FROM user_tags GROUP BY user_id;
