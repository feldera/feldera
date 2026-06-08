CREATE VIEW having_021 AS
SELECT v + 1 FROM hav GROUP BY ALL HAVING avg(try_add(v, 1) + 1) = 1;
