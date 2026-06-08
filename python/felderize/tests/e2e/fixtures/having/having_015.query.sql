CREATE VIEW having_015 AS
SELECT sum(v) FROM hav HAVING avg(try_add(v, 1)) = 1;
