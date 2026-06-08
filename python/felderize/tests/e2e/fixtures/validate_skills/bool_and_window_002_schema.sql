-- rule: bool_and_window
-- spark: every(bool_col) OVER (ORDER BY bool_col) / bool_and(bool_col) OVER (ORDER BY bool_col) — boolean aggregate used as window function with ORDER BY on a BOOLEAN column
-- feldera: UNSUPPORTED — Feldera compiler error: 'OVER currently cannot sort on columns with type BOOL'. Mark as unsupported when the window ORDER BY is on a BOOLEAN column.
CREATE TABLE status_log (
  log_id INT,
  is_valid BOOLEAN,
  priority INT
);
