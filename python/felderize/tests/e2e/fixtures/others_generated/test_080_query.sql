CREATE OR REPLACE TEMP VIEW val73_second_score AS
SELECT
  grp,
  item_id,
  score,
  nth_value(score, 2) OVER (
    PARTITION BY grp
    ORDER BY created_at
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS second_score
FROM window_scores;
