-- rule: rank_topk
-- spark: RANK() OVER (PARTITION BY ... ORDER BY ...) — same TopK restriction
-- feldera: RANK() same; must be in subquery with WHERE filter on result
CREATE OR REPLACE TEMP VIEW top_scorers_v3 AS SELECT student_id, subject, score, academic_year FROM (SELECT student_id, subject, score, academic_year, RANK() OVER (PARTITION BY subject, academic_year ORDER BY score DESC) AS score_rank FROM student_scores_v3) sub WHERE sub.score_rank <= 2;
