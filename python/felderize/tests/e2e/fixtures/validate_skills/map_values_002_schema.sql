-- rule: map_values
-- spark: map_values(m) — extract values of a map as array
-- feldera: MAP_VALUES(m)
CREATE TABLE employee_scores (emp_id INT, score_map MAP<STRING, DOUBLE>);
