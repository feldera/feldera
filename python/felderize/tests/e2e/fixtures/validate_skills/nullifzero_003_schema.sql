-- rule: nullifzero
-- spark: NULLIFZERO(x) — return NULL if x is 0
-- feldera: NULLIF(x, 0)
CREATE TABLE performance_scores (employee_id INT, bonus_points INT, penalty_points INT);
