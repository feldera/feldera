-- rule: map_values
-- spark: map_values(m) — extract values of a map as array
-- feldera: MAP_VALUES(m)
CREATE OR REPLACE TEMP VIEW score_values_v2 AS SELECT emp_id, map_values(score_map) AS score_array FROM employee_scores;
