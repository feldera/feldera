CREATE VIEW having_016 AS
SELECT sum(v) FROM hav HAVING sum(try_add(v, 1)) = 1;
