CREATE VIEW having_020 AS
SELECT v + 1 FROM hav GROUP BY ALL HAVING avg(try_add(v, 1)) = 1;
