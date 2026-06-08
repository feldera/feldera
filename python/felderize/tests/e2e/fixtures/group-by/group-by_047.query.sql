CREATE VIEW group-by_047 AS
SELECT count(*)
FROM VALUES (ARRAY(MAP(1, 2, 2, 3), MAP(1, 3))), (ARRAY(MAP(2, 3), MAP(1, 3))), (ARRAY(MAP(2, 3, 1, 2), MAP(1, 3))) as t(a)
GROUP BY a;
