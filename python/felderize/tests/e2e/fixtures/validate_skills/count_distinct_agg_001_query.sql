-- rule: count_distinct_agg
-- spark: COUNT(DISTINCT col) — count distinct values in a group
-- feldera: COUNT(DISTINCT col) — works identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW event_summary_v1 AS SELECT user_id, COUNT(DISTINCT event_type) AS distinct_events FROM user_events GROUP BY user_id;
