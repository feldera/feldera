CREATE VIEW having_019 AS
SELECT sum(v) FROM hav GROUP BY v HAVING sum(ifnull(v, 1)) = 1;
