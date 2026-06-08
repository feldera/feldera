CREATE OR REPLACE TEMP VIEW val76_group_max_score AS
SELECT
  grp,
  item_id,
  score,
  MAX(score) OVER (PARTITION BY grp) AS grp_max_score
FROM window_scores;
