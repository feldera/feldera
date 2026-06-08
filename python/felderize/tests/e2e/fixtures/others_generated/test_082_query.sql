CREATE OR REPLACE TEMP VIEW val75_prev_score_default AS
SELECT
  grp,
  item_id,
  score,
  lag(score, 1, CAST(0 AS DECIMAL(12,2))) OVER (PARTITION BY grp ORDER BY created_at) AS prev_score
FROM window_scores;
