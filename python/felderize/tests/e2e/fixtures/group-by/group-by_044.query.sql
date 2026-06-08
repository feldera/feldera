CREATE VIEW group-by_044 AS
SELECT
  a,
  collect_list(b),
  array_agg(b)
FROM VALUES
  (1,4),(2,3),(1,4),(2,4) AS v(a,b)
GROUP BY a;
