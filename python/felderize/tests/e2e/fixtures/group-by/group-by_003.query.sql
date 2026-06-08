CREATE VIEW group-by_003 AS
SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(*)
  FROM (VALUES (1, 1), (2, 2), (2, 2)) t(x, y);
