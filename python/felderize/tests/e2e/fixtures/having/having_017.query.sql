CREATE VIEW having_017 AS
SELECT sum(v) FROM hav HAVING sum(ifnull(v, 1)) = 1;
