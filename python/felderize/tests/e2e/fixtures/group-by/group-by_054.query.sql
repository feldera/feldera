CREATE VIEW group-by_054 AS
SELECT count(*)
FROM VALUES (ARRAY(map(array(1, 2), 2, array(2, 3), 3))), (ARRAY(MAP(ARRAY(1, 3), 3))), (ARRAY(map(array(2, 3), 3, array(1, 2), 2))) as t(a)
group BY a;
