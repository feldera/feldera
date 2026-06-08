CREATE OR REPLACE TEMP VIEW bm28_exam_percentiles AS
SELECT class_id, student_id, score,
  PERCENT_RANK() OVER (PARTITION BY class_id ORDER BY score) AS pct_rank,
  CUME_DIST() OVER (PARTITION BY class_id ORDER BY score) AS cumulative_dist
FROM exam_scores;
