CREATE VIEW group-by_052 AS
SELECT count(*)
FROM VALUES (map(array(1, 2), 2, array(2, 3), 3)), (map(array(1, 3), 3)), (map(array(2, 3), 3, array(1, 2), 2)) as t(a)
group BY a;
