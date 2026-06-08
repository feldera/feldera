-- rule: UNPIVOT
-- spark: UNPIVOT (val FOR col IN (c1, c2, c3)) — rotate columns into rows
-- feldera: same — pass through unchanged
CREATE VIEW unpivot_001_v AS
SELECT id, quarter, amount
FROM sales
UNPIVOT (amount FOR quarter IN (q1, q2, q3));
