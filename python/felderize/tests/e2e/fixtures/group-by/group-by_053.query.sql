CREATE VIEW group-by_053 AS
SELECT count(*)
FROM VALUES (map(array(1, 2, 3), 3)), (map(array(3, 2, 1), 3)) as t(a)
group BY a;
