-- rule: cube_agg
-- spark: CUBE(a, b) — all combinations of grouping
-- feldera: CUBE(a, b) — same
CREATE TABLE employee_hours (
  department STRING,
  shift STRING,
  hours_worked INT
);
