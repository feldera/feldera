CREATE VIEW group-by_046 AS
SELECT col1, count(*) AS cnt
FROM VALUES
  (0.0),
  (-0.0),
  (double('NaN')),
  (double('NaN')),
  (double('Infinity')),
  (double('Infinity')),
  (-double('Infinity')),
  (-double('Infinity'))
GROUP BY col1
ORDER BY col1;
