-- rule: UNPIVOT
-- spark: UNPIVOT (val FOR col IN (c1, c2, c3)) — rotate columns into rows
-- feldera: same — pass through unchanged
CREATE TABLE sales (id INT, q1 INT, q2 INT, q3 INT);
