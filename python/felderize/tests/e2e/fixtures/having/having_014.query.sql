CREATE VIEW having_014 AS
SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2 ORDER BY avg(v);
