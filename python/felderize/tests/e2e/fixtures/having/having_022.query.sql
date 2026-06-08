CREATE VIEW having_022 AS
SELECT sum(v) FROM hav GROUP BY ifnull(v, 1) + 1 order by ifnull(v, 1) + 1;
