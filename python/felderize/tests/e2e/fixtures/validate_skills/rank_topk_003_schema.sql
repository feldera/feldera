-- rule: rank_topk
-- spark: RANK() OVER (PARTITION BY ... ORDER BY ...) — same TopK restriction
-- feldera: RANK() same; must be in subquery with WHERE filter on result
CREATE TABLE student_scores_v3 (student_id INT, subject STRING, score INT, academic_year INT);
