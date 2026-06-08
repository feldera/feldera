CREATE VIEW having_018 AS
SELECT sum(v) FROM hav GROUP BY ALL HAVING sum(ifnull(v, 1)) = 1;
