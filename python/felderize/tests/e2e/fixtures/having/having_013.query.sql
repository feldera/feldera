CREATE VIEW having_013 AS
SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2 ORDER BY sum(v);
