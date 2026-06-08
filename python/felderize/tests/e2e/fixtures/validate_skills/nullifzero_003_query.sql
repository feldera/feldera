-- rule: nullifzero
-- spark: NULLIFZERO(x) — return NULL if x is 0
-- feldera: NULLIF(x, 0)
CREATE OR REPLACE TEMP VIEW performance_nullif_v3 AS SELECT employee_id, NULLIFZERO(bonus_points) AS bonus_or_null, NULLIFZERO(penalty_points) AS penalty_or_null FROM performance_scores;
