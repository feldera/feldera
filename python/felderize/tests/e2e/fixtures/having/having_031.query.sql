CREATE VIEW having_031 AS
SELECT SUM(v) + 1 + MIN(v) FROM hav HAVING 1 + 1 + 1 + MIN(v) + 1 + SUM(v);
