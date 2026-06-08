CREATE OR REPLACE TEMP VIEW val74_next_score_default AS
SELECT
  grp,
  item_id,
  score,
  lead(score, 1, CAST(0 AS DECIMAL(12,2))) OVER (PARTITION BY grp ORDER BY created_at) AS next_score
FROM window_scores;
