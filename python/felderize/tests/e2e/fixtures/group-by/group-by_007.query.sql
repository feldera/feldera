CREATE VIEW group-by_007 AS
SELECT id FROM range(10) HAVING id > 0;
