-- rule: collect_set_agg
-- spark: collect_set(col) — aggregate distinct column values per group into an array
-- feldera: ARRAY_AGG(DISTINCT col)
CREATE TABLE user_tags (user_id INT, tag VARCHAR(50));
CREATE TABLE user_tag_summary (user_id INT, tag_list ARRAY<VARCHAR(50)>);
