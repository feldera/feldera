CREATE VIEW having_002 AS
SELECT count(k) FROM hav GROUP BY v + 1 HAVING v + 1 = 2;
